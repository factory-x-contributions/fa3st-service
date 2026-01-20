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

import java.io.InputStream;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.crypto.dsig.XMLSignature;
import javax.xml.crypto.dsig.XMLSignatureFactory;
import javax.xml.crypto.dsig.dom.DOMValidateContext;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class TrustedListService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrustedListService.class);

    private final Map<String, TrustedIssuerMetadata> issuers = new ConcurrentHashMap<>();

    private final X509Certificate fxRootCertificate; // Factory-X Operating Company root / TSL signing cert
    private final URL trustedListUrl;

    public TrustedListService(X509Certificate fxRootCertificate, URL trustedListUrl) {
        this.fxRootCertificate = fxRootCertificate;
        this.trustedListUrl = trustedListUrl;
        reloadTrustedList(); // call at startup; schedule periodically if needed
    }


    public boolean isTrustedIssuer(String issuer) {
        return issuers.containsKey(issuer);
    }


    public TrustedIssuerMetadata getIssuerMetadata(String issuer) {
        return issuers.get(issuer);
    }


    public synchronized void reloadTrustedList() {
        try (InputStream is = trustedListUrl.openStream()) {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            Document doc = dbf.newDocumentBuilder().parse(is);

            if (!verifySignature(doc)) {
                throw new SecurityException("Trusted List XML signature invalid");
            }

            Map<String, TrustedIssuerMetadata> parsedIssuers = parseIssuers(doc);
            issuers.clear();
            issuers.putAll(parsedIssuers);

            LOGGER.info("Reloaded Trusted List:" + issuers.size() + "trusted issuers");
        }
        catch (Exception e) {
            LOGGER.error("Failed to reload Trusted List " + e);
        }
    }


    private boolean verifySignature(Document doc) throws Exception {
        NodeList nl = doc.getElementsByTagNameNS(XMLSignature.XMLNS, "Signature");
        if (nl.getLength() == 0) {
            throw new IllegalStateException("No XML Signature element found in Trusted List");
        }

        XMLSignatureFactory fac = XMLSignatureFactory.getInstance("DOM");
        DOMValidateContext valContext = new DOMValidateContext(fxRootCertificate.getPublicKey(), nl.item(0));

        XMLSignature signature = fac.unmarshalXMLSignature(valContext);
        boolean coreValidity = signature.validate(valContext);

        if (!coreValidity) {
            LOGGER.warn("Trusted List XML Signature is NOT valid");
        }
        return coreValidity;
    }


    private Map<String, TrustedIssuerMetadata> parseIssuers(Document doc) throws Exception {
        Map<String, TrustedIssuerMetadata> result = new HashMap<>();

        XPathFactory xpf = XPathFactory.newInstance();
        XPath xpath = xpf.newXPath();

        String expression = "//*[local-name()='TrustServiceProvider']/*[local-name()='TSPServices']/*[local-name()='TSPService']/*[local-name()='ServiceName']";
        NodeList serviceNameNodes = (NodeList) xpath.evaluate(expression, doc, XPathConstants.NODESET);

        for (int i = 0; i < serviceNameNodes.getLength(); i++) {
            Element serviceNameEl = (Element) serviceNameNodes.item(i);
            String serviceName = serviceNameEl.getTextContent().trim();

            if (serviceName.startsWith("https://")) {
                String issuer = serviceName;
                String jwksEndpoint = issuer.endsWith("/") ? issuer + "sts/jwks" : issuer + "/sts/jwks";

                result.put(issuer, new TrustedIssuerMetadata(issuer, issuer, jwksEndpoint));
                LOGGER.debug("Found trusted issuer: {}", issuer);
            }
        }

        LOGGER.debug("Parsed {} trusted issuers from TSL", result.size());
        return result;
    }
}
