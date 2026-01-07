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

public class TrustedIssuerMetadata {
    private final String issuer;
    private final String stsBaseUrl; // e.g. "https://sts.consumer.com"
    private final String jwksEndpoint; // e.g. "https://sts.consumer.com/sts/jwks"
    // X509Certificate tlsCertificate;

    public TrustedIssuerMetadata(String issuer, String stsBaseUrl, String jwksEndpoint) {
        this.issuer = issuer;
        this.stsBaseUrl = stsBaseUrl;
        this.jwksEndpoint = jwksEndpoint;
    }


    public String getIssuer() {
        return issuer;
    }


    public String getStsBaseUrl() {
        return stsBaseUrl;
    }


    public String getJwksEndpoint() {
        return jwksEndpoint;
    }
}
