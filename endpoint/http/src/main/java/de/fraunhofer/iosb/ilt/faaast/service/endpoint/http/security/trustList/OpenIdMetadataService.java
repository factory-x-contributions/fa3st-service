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
package de.fraunhofer.iosb.ilt.faaast.service.endpoint.http.security.trustList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.faaast.service.util.SslHelper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.LoggerFactory;


public class OpenIdMetadataService {
    private final Map<String, String> jwksUriCache = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OpenIdMetadataService.class);

    public String getJwksUriForIssuer(String issuer) {
        return jwksUriCache.computeIfAbsent(issuer, this::fetchJwksUri);
    }


    private String fetchJwksUri(String issuer) {
        String[] candidates = new String[] {
                issuer.endsWith("/") ? issuer + ".well-known/openid-configuration"
                        : issuer + "/.well-known/openid-configuration",
                issuer.endsWith("/") ? issuer + ".well-known/oauth-authorization-server"
                        : issuer + "/.well-known/oauth-authorization-server"
        };

        for (String url: candidates) {
            try {
                HttpClient client = SslHelper.newClientAcceptingAllCertificates();
                HttpRequest request = HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(url))
                        .build();

                // Send request and get response body as String
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    String body = response.body();
                    String jwksUri = parseJwksUriFromJson(body);
                    if (jwksUri != null && !jwksUri.isBlank()) {
                        return jwksUri;
                    }
                }
            }
            catch (NoSuchAlgorithmException | KeyManagementException | IOException e) {
                LOGGER.warn("Error getting OpenID metadata from {}", url, e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore interrupt flag
                throw new IllegalStateException("Request interrupted while fetching OpenID metadata from " + url, e);
            }
        }

        throw new IllegalStateException("No jwks_uri found via well-known metadata for issuer " + issuer);
    }


    private String parseJwksUriFromJson(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode jwksUriNode = root.get("jwks_uri");
            return jwksUriNode != null && !jwksUriNode.isNull()
                    ? jwksUriNode.asText()
                    : null;
        }
        catch (IOException e) {
            return null;
        }
    }
}
