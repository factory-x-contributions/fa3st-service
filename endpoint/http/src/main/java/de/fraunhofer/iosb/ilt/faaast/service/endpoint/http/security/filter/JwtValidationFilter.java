/*
 * Copyright (c) 2021 Fraunhofer IOSB, eine rechtlich nicht selbstaendige
 * Einrichtung der Fraunhofer-Gesellschaft zur Foerderung der angewandten
 * Forschung e.V.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.filter;

import com.auth0.jwk.InvalidPublicKeyException;
import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.auth.AuthState;
import de.fraunhofer.iosb.ilt.faaast.service.util.SslHelper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.util.Collections;
import java.util.Objects;
import org.slf4j.LoggerFactory;


/**
 * Filters any incoming request by verifying its JWT if available.
 * If no Authorization: Bearer <...> header is available, assumes an anonymous request.
 */
public class JwtValidationFilter extends JwtAuthorizationFilter {

    private static final String AUTHORIZATION_KWD = "Authorization";
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JwtValidationFilter.class);

    private JwkProvider jwkProvider = null;
    private String jwkProviderUrl = null;

    public JwtValidationFilter(JwkProvider jwkProvider) {
        this.jwkProvider = jwkProvider;
    }


    public JwtValidationFilter(String jwkProviderUrl) {
        this.jwkProviderUrl = jwkProviderUrl;
        try {
            this.jwkProvider = new UrlJwkProvider(new URL(jwkProviderUrl + "/jwks"));
        }
        catch (MalformedURLException e) {
            LOGGER.error("WARNING: Could not create JWK Provider.");
        }
    }


    /**
     * If a bearer token (JWT) is passed as header, validates this token and blocks the request as unauthenticated.
     *
     * @param servletRequest the <code>ServletRequest</code> object contains the client's request
     * @param servletResponse the <code>ServletResponse</code> object contains the filter's response
     * @param filterChain the <code>FilterChain</code> for invoking the next filter or the resource
     * @throws IOException could not write HttpResponse body OR exception in next filter steps
     * @throws ServletException exception in next filter steps
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException,
            ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        // If multiple Authorization headers are present, attackers could maybe use one for auth and one for claims
        var authHeaders = httpRequest.getHeaders(AUTHORIZATION_KWD);
        var authHeaderList = Collections.list(authHeaders);
        if (authHeaderList.size() > 1) {
            LOGGER.debug("Multiple authorization headers present! Not authorizing request.");
            respondForbidden(httpResponse);
            return;
        }

        if (httpRequest.getHeader(AUTHORIZATION_KWD) == null) {
            // No JWT in request, anonymous requestor
            servletRequest.setAttribute("auth.state", AuthState.ANONYMOUS);
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        DecodedJWT jwt = extractAndDecodeJwt(httpRequest);

        // Do Token Exchange
        try {
            if (Objects.nonNull(jwkProviderUrl)) {
                HttpClient client = SslHelper.newClientAcceptingAllCertificates();

                String form = "grant_type=" + URLEncoder.encode("urn:ietf:params:oauth:grant-type:token-exchange", StandardCharsets.UTF_8) +
                        "&subject_token_type=" + URLEncoder.encode("urn:ietf:params:oauth:token-type:jwt", StandardCharsets.UTF_8) +
                        "&requested_token_type=" + URLEncoder.encode("urn:ietf:params:oauth:token-type:access_token", StandardCharsets.UTF_8) +
                        "&subject_token=" + jwt.getToken() +
                        "&audience=" + URLEncoder.encode("fa3st", StandardCharsets.UTF_8);

                HttpRequest request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(form))
                        .uri(URI.create(jwkProviderUrl + "/token"))
                        .build();

                // Send request and get response body as String
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    jwt = extractAndDecodeJwt(response);
                }
                else {
                    LOGGER.error("Token exchange failed, continue with previous token.");
                }
            }
        }
        catch (InterruptedException | KeyManagementException | NoSuchAlgorithmException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not exchange token with provider sts.", e);
        }

        if (jwt == null || !validateJWT(jwt)) {
            LOGGER.debug("Could not extract and validate JWT");
            respondForbidden(httpResponse);
        }
        else {
            // Continue with the request as authenticated
            servletRequest.setAttribute("auth.state", AuthState.AUTHENTICATED);
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }


    private static void respondForbidden(HttpServletResponse httpResponse) throws IOException {
        httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
        httpResponse.getWriter().write("Invalid token");
    }


    private boolean validateJWT(DecodedJWT decodedJWT) {
        Jwk jwk;
        try {
            jwk = jwkProvider.get(decodedJWT.getKeyId());
        }
        catch (JwkException getJwkException) {
            LOGGER.debug("Could not get JWK from JWT. Not authorizing request.", getJwkException);
            return false;
        }
        Algorithm algorithm;
        try {
            algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
        }
        catch (InvalidPublicKeyException invalidPublicKeyException) {
            LOGGER.debug("InvalidPublicKeyException when reading public key from JWT. Not authorizing request", invalidPublicKeyException);
            return false;
        }

        try {
            algorithm.verify(decodedJWT);
        }
        catch (SignatureVerificationException signatureVerificationException) {
            LOGGER.debug("Could not verify JWT using algorithm {}", algorithm.getName());
            return false;
        }
        JWTVerifier verifier = JWT.require(algorithm).build();

        try {
            verifier.verify(decodedJWT);
        }
        catch (JWTVerificationException verificationException) {
            LOGGER.debug("Could not verify JWT");
            return false;
        }

        return true;
    }
}
