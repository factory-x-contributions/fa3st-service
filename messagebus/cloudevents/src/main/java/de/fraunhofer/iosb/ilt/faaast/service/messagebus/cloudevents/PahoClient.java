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
package de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import de.fraunhofer.iosb.ilt.faaast.service.config.CertificateConfig;
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.*;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Eclipse Paho MQTT client that:
 * fetches an OAuth2 access token (client credentials) from config.getIdentityProviderUrl()
 * using client credentials from config.getClientId(), getClientSecret;
 * connects over WebSocket and sends Authorization: Bearer token header.
 * refreshes token proactively and updates headers so auto-reconnect uses a valid token.
 */
public class PahoClient {

    private static final Logger logger = LoggerFactory.getLogger(PahoClient.class);

    private final MessageBusCloudeventsConfig config;
    private MqttClient mqttClient;
    private MqttConnectOptions connectOptions;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private volatile String accessToken;
    private volatile Instant tokenExpiry;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PahoClient-TokenRefresher");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> refreshTask;
    private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    public PahoClient(MessageBusCloudeventsConfig config) {
        this.config = config;
    }


    /**
     * Starts the client connection.
     *
     * @throws MessageBusException if message bus fails to start
     */
    public void start() throws MessageBusException {
        connectOptions = new MqttConnectOptions();
        try {
            if (Objects.nonNull(config.getClientCertificate())
                    && Objects.nonNull(config.getClientCertificate().getKeyStorePath())
                    && !config.getClientCertificate().getKeyStorePath().isEmpty()) {
                connectOptions.setSocketFactory(getSSLSocketFactory(config.getClientCertificate()));
            }
        }
        catch (GeneralSecurityException | IOException e) {
            throw new MessageBusException("error setting up SSL for Cloudevents MQTT message bus", e);
        }

        if (!Objects.isNull(config.getUser())) {
            connectOptions.setUserName(config.getUser());
            connectOptions.setPassword(config.getPassword() != null
                    ? config.getPassword().toCharArray()
                    : new char[0]);
        }

        connectOptions.setAutomaticReconnect(true);
        connectOptions.setCleanSession(false);

        if (!Objects.isNull(config.getIdentityProviderUrl())) {
            try {
                refreshAccessTokenIfNeeded(true);
                applyAuthorizationHeader(connectOptions, accessToken);
                config.setUser("");
            }
            catch (Exception e) {
                throw new MessageBusException("Failed to obtain OAuth token from Identity Provider", e);
            }
        }

        try {
            mqttClient = new MqttClient(
                    config.getHost(),
                    config.getClientId(),
                    new MemoryPersistence());
            mqttClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectionLost(Throwable throwable) {
                    logger.warn("Cloudevents MQTT message bus connection lost", throwable);
                    if (!Objects.isNull(config.getIdentityProviderUrl())) {
                        try {
                            refreshAccessTokenIfNeeded(false);
                            applyAuthorizationHeader(connectOptions, accessToken);
                        }
                        catch (Exception e) {
                            logger.warn("Failed to refresh token on connectionLost", e);
                        }
                    }
                }


                @Override
                public void deliveryComplete(IMqttDeliveryToken imdt) {
                    // intentionally left empty
                }


                @Override
                public void messageArrived(String string, MqttMessage mm) throws Exception {
                    // intentionally left empty
                }


                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    logger.debug("Cloudevents MQTT MessageBus Client connected to broker. reconnect={}", reconnect);
                    if (!Objects.isNull(config.getIdentityProviderUrl())) {
                        scheduleProactiveRefresh();
                    }
                }
            });

            logger.trace("connecting to Cloudevents MQTT broker: {}", config.getHost());
            mqttClient.connect(connectOptions);
            logger.debug("connected to Cloudevents MQTT broker: {}", config.getHost());
        }
        catch (MqttException e) {
            throw new MessageBusException("Failed to connect to Cloudevents MQTT server", e);
        }
    }


    /**
     * Stops the client connection.
     */
    public void stop() {
        if (mqttClient == null) {
            cancelRefreshTask();
            return;
        }
        try {
            if (mqttClient.isConnected()) {
                logger.trace("disconnecting from Cloudevents MQTT broker...");
                mqttClient.disconnect();
                logger.info("disconnected from Cloudevents MQTT broker");
            }
            logger.trace("closing paho-client");
            mqttClient.close(true);
            mqttClient = null;
        }
        catch (MqttException e) {
            logger.debug("Cloudevents MQTT message bus did not stop gracefully", e);
        }
        finally {
            cancelRefreshTask();
        }
    }


    private SSLSocketFactory getSSLSocketFactory(CertificateConfig certificate) throws GeneralSecurityException, IOException {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }


                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        // Trust all client certificates
                    }


                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        // Trust all server certificates
                    }
                }
        };
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        return sslContext.getSocketFactory();
    }


    /**
     * Publishes the message.
     *
     * @param topic the topic to publish on
     * @param content the message to publish
     * @throws MessageBusException if publishing the message fails
     */
    public void publish(String topic, String content) throws MessageBusException {
        if (mqttClient == null || !mqttClient.isConnected()) {
            logger.debug("received data but Cloudevents MQTT connection is closed, trying to connect...");
            start();
        }
        MqttMessage msg = new MqttMessage(content.getBytes());
        try {
            mqttClient.publish(topic, msg);
            logger.info("message published - topic: {}, data: {}", topic, content);
        }
        catch (MqttException e) {
            throw new MessageBusException("publishing message on Cloudevents MQTT message bus failed", e);
        }
    }


    /**
     * Subscribe to a mqtt topic.
     *
     * @param topic the topic to subscribe to
     * @param listener the callback listener
     */
    public void subscribe(String topic, IMqttMessageListener listener) {
        try {
            mqttClient.subscribe(topic, listener);
        }
        catch (MqttException e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * Unsubscribe from a mqtt topic.
     *
     * @param topic the topic to unsubscribe from
     */
    public void unsubscribe(String topic) {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                mqttClient.unsubscribe(topic);
            }
            catch (MqttException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    // ---------- OAuth ----------


    private void applyAuthorizationHeader(MqttConnectOptions options, String token) {
        options.setPassword(token.toCharArray());
        logger.debug("Applied token as password");
    }


    private synchronized void refreshAccessTokenIfNeeded(boolean force) throws Exception {
        Instant now = Instant.now();
        boolean expiredOrMissing = accessToken == null ||
                tokenExpiry == null ||
                !now.isBefore(tokenExpiry);
        if (!force && !expiredOrMissing) {
            return;
        }
        TokenResponse tr = requestClientCredentialsToken();
        this.accessToken = tr.accessToken;
        // Use a 60s safety margin
        this.tokenExpiry = now.plusSeconds(Math.max(0, tr.expiresIn - 60));
        logger.debug("Cloudevent Publisher obtained new access token, expires in ~{} seconds", tr.expiresIn);
    }


    private void scheduleProactiveRefresh() {
        cancelRefreshTask();
        long delaySec = Math.max(60, Duration.between(Instant.now(), tokenExpiry.minusSeconds(30)).getSeconds());
        refreshTask = scheduler.schedule(() -> {
            try {
                refreshAccessTokenIfNeeded(true);
                if (connectOptions != null) {
                    applyAuthorizationHeader(connectOptions, accessToken);
                }
            }
            catch (Exception e) {
                logger.warn("Proactive token refresh failed, will retry in 60s", e);
                scheduler.schedule(this::scheduleProactiveRefresh, 60, TimeUnit.SECONDS);
                return;
            }
            scheduleProactiveRefresh();
        }, delaySec, TimeUnit.SECONDS);
    }


    private void cancelRefreshTask() {
        if (refreshTask != null) {
            refreshTask.cancel(true);
            refreshTask = null;
        }
    }


    private TokenResponse requestClientCredentialsToken() throws Exception {
        String tokenEndpoint = Objects.requireNonNull(config.getIdentityProviderUrl(), "IdentityProviderUrl must not be null");

        StringBuilder body = new StringBuilder();
        body.append("grant_type=client_credentials");
        body.append("&client_id=").append(URLEncoder.encode(config.getClientId(), StandardCharsets.UTF_8));
        body.append("&client_secret=").append(URLEncoder.encode(config.getClientSecret(), StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder(URI.create(tokenEndpoint))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Token request failed: HTTP " + response.statusCode() + " - " + response.body());
        }

        return parseTokenResponse(response.body());
    }


    private TokenResponse parseTokenResponse(String body) throws IOException {
        final TokenResponse tr;
        try {
            tr = MAPPER.readValue(body, TokenResponse.class);
        }
        catch (JsonProcessingException e) {
            throw new IOException("Failed to parse token response: " + body, e);
        }
        if (tr == null || tr.accessToken == null || Objects.isNull(tr.expiresIn)) {
            throw new IOException("Failed to parse token response (missing fields): " + body);
        }
        return tr;
    }

    /**
     * Class for Response containing token and expiresIn as well as other stuff not being read.
     */
    public static class TokenResponse {
        public String accessToken;
        public Long expiresIn;
    }
}
