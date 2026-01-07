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
import de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.trustList.OpenIdMetadataService;
import de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.trustList.TrustedListService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.LoggerFactory;


/**
 * Filters any incoming request by verifying its JWT if available.
 * If no Authorization: Bearer <...> header is available, assumes an anonymous request.
 */
public class JwtValidationFilter extends JwtAuthorizationFilter {

    private static final String AUTHORIZATION_KWD = "Authorization";
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JwtValidationFilter.class);
    private final TrustedListService trustedListService;
    private final OpenIdMetadataService metadataService;
    private final ConcurrentMap<String, JwkProvider> jwkProviders = new ConcurrentHashMap<>();

    public JwtValidationFilter(TrustedListService trustedListService) {
        this.trustedListService = trustedListService;
        this.metadataService = new OpenIdMetadataService();
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

        // Extract JWT
        DecodedJWT jwt = extractAndDecodeJwt(httpRequest);
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
        String issuer = decodedJWT.getIssuer();
        if (issuer == null || issuer.isBlank()) {
            return false;
        }

        // 1. Check issuer against Trusted List (by domain)
        if (!trustedListService.isTrustedIssuer(issuer)) {
            LOGGER.debug("Issuer '{}' not trusted according to FX Trusted List", issuer);
            return false;
        }

        // 2. Get jwks_uri through well-known metadata (OIDC Discovery / RFC 8414)
        String jwksUri;
        try {
            jwksUri = metadataService.getJwksUriForIssuer(issuer);
        }
        catch (Exception e) {
            LOGGER.debug("Could not discover jwks_uri for issuer {}", issuer, e);
            return false;
        }

        // 3. Get or create JwkProvider for this jwks_uri
        JwkProvider jwkProvider = jwkProviders.computeIfAbsent(jwksUri, uri -> {
            try {
                return new UrlJwkProvider(new URL(uri));
            }
            catch (MalformedURLException e) {
                LOGGER.error("Invalid jwks_uri '{}' for issuer {}", uri, issuer, e);
                return null;
            }
        });

        if (jwkProvider == null) {
            return false;
        }

        // 3. Get JWK using kid from token
        Jwk jwk;
        try {
            jwk = jwkProvider.get(decodedJWT.getKeyId());
        }
        catch (JwkException getJwkException) {
            LOGGER.debug("Could not get JWK (kid={}) from JWKS endpoint {}. Not authorizing request.",
                    decodedJWT.getKeyId(), jwksUri, getJwkException);
            return false;
        }

        // 4. Build Algorithm from public key
        Algorithm algorithm;
        try {
            algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
        }
        catch (InvalidPublicKeyException invalidPublicKeyException) {
            LOGGER.debug("Invalid public key from JWKS endpoint {}. Not authorizing request.",
                    jwksUri, invalidPublicKeyException);
            return false;
        }

        // 5. Verify signature
        try {
            algorithm.verify(decodedJWT);
        }
        catch (SignatureVerificationException signatureVerificationException) {
            LOGGER.debug("Could not verify JWT signature using algorithm {} for issuer {}",
                    algorithm.getName(), issuer, signatureVerificationException);
            return false;
        }

        // 6. Verify standard claims (exp, iat, iss, etc.)
        JWTVerifier verifier = JWT.require(algorithm)
                .withIssuer(issuer) // ensure 'iss' matches what we trusted
                // optionally: .withAudience("expected-audience")
                .build();

        try {
            verifier.verify(decodedJWT);
        }
        catch (JWTVerificationException verificationException) {
            LOGGER.debug("JWT claims validation failed for issuer {}.", issuer, verificationException);
            return false;
        }

        return true;
    }
}
