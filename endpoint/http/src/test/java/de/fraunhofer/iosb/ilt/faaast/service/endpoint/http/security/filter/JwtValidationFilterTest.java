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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.trustList.OpenIdMetadataService;
import de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.trustList.TrustedListService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.ConcurrentMap;
import org.junit.Test;


public class JwtValidationFilterTest extends JwtAuthorizationFilterTest {

    private JwtValidationFilter filter;

    @Test
    public void jwtIsVerified() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();

        RSAPublicKey publicKey = (RSAPublicKey) kp.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) kp.getPrivate();

        String kid = "unit-test-kid";
        String issuer = "https://someIssuer.com";
        String fakeJwksUri = "https://fake.jwks.example.com"; // Arbitrary URI used for provider caching

        // Create signed JWT
        String jwt = JWT.create()
                .withKeyId(kid)
                .withIssuer(issuer)
                .sign(Algorithm.RSA256(publicKey, privateKey));

        // Mock JWK
        Jwk jwk = mock(Jwk.class);
        when(jwk.getPublicKey()).thenReturn(publicKey);
        when(jwk.getId()).thenReturn(kid);

        // Mock JwkProvider
        JwkProvider mockJwkProvider = mock(JwkProvider.class);
        when(mockJwkProvider.get(kid)).thenReturn(jwk);

        // Mock dependencies
        TrustedListService trustedListService = mock(TrustedListService.class);
        when(trustedListService.isTrustedIssuer(issuer)).thenReturn(true);

        OpenIdMetadataService metadataService = mock(OpenIdMetadataService.class);
        when(metadataService.getJwksUriForIssuer(issuer)).thenReturn(fakeJwksUri);

        // Create the filter normally
        JwtValidationFilter filter = new JwtValidationFilter(trustedListService);

        Field metadataField = JwtValidationFilter.class.getDeclaredField("metadataService");
        metadataField.setAccessible(true);
        metadataField.set(filter, metadataService);

        Field jwkProvidersField = JwtValidationFilter.class.getDeclaredField("jwkProviders");
        jwkProvidersField.setAccessible(true);
        ConcurrentMap<String, JwkProvider> jwkProviders = (ConcurrentMap<String, JwkProvider>) jwkProvidersField.get(filter);
        jwkProviders.put(fakeJwksUri, mockJwkProvider);

        // Setup request/response/chain mocks (assuming mockRequest, mockResponse, mockFilterChain from parent)
        HttpServletRequest request = mockRequest("GET", "/api/v3.0/submodels", jwt);
        HttpServletResponse response = mockResponse();
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        FilterChain filterChain = mockFilterChain();

        // Execute filter
        filter.doFilter(request, response, filterChain);

        // Assertions: filter chain proceeded (valid JWT)
        verify(filterChain, times(1)).doFilter(any(), any());

        // Verify metadata was queried
        verify(metadataService, times(1)).getJwksUriForIssuer(issuer);

        // Verify JWK provider was used
        verify(mockJwkProvider, times(1)).get(kid);
    }

}
