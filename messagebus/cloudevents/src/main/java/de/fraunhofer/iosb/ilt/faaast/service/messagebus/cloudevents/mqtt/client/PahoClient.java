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
package de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mqtt.client;

import de.fraunhofer.iosb.ilt.faaast.service.config.CertificateConfig;
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mqtt.client.config.MqttClientConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Objects;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper for Eclipse Paho MQTT client.
 */
public abstract class PahoClient {

    private static final Logger logger = LoggerFactory.getLogger(PahoClient.class);

    private final MqttClientConfig config;
    private org.eclipse.paho.client.mqttv3.MqttClient pahoClient;
    private MqttConnectOptions connectOptions;

    public PahoClient(MqttClientConfig config) {
        this.config = config;
    }


    /**
     * Starts the client connection.
     *
     * @throws MessageBusException if the client fails to connect to the broker
     */
    public void start() throws MessageBusException {
        connectOptions = buildConnectOptions();

        try {
            pahoClient = new org.eclipse.paho.client.mqttv3.MqttClient(
                    config.host(),
                    config.clientId(),
                    new MemoryPersistence());
            pahoClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectionLost(Throwable throwable) {
                    logger.warn("Connection to MQTT broker {} lost", config.host(), throwable);
                }


                @Override
                public void deliveryComplete(IMqttDeliveryToken imdt) {
                    logger.trace("MQTT message delivered");
                }


                @Override
                public void messageArrived(String string, MqttMessage mm) {
                    // intentionally left empty
                }


                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    logger.debug("Connected to broker. reconnect={}", reconnect);
                }
            });

            logger.trace("Connecting to MQTT broker: {}", config.host());

            pahoClient.connect(connectOptions);
        }
        catch (MqttException e) {
            throw new MessageBusException("Failed to connect to Cloudevents MQTT server", e);
        }
    }


    /**
     * Set the current password to connect to the MQTT broker.
     *
     * @param password A valid password to connect to the MQTT broker
     */
    protected final void setPassword(String password) {
        connectOptions.setPassword(password.toCharArray());
    }


    /**
     * Build the connect options object necessary for the paho client.
     * 
     * @return connect options containing information about MQTT connection.
     * @throws MessageBusException Setting SSL socket factory failed for client certificate.
     */
    protected MqttConnectOptions buildConnectOptions() throws MessageBusException {
        connectOptions = new MqttConnectOptions();
        try {
            if (Objects.nonNull(config.clientCertificate())
                    && Objects.nonNull(config.clientCertificate().getKeyStorePath())
                    && !config.clientCertificate().getKeyStorePath().isEmpty()) {
                connectOptions.setSocketFactory(getSSLSocketFactory(config.clientCertificate()));
            }
        }
        catch (GeneralSecurityException | IOException e) {
            throw new MessageBusException("error setting up SSL for Cloudevents MQTT message bus", e);
        }

        connectOptions.setAutomaticReconnect(true);
        connectOptions.setCleanSession(false);

        connectOptions.setUserName(config.user());

        return connectOptions;
    }


    /**
     * Stops the client connection.
     */
    public void stop() {
        if (pahoClient == null) {
            return;
        }
        try {
            if (pahoClient.isConnected()) {
                logger.trace("Disconnecting from MQTT broker...");
                pahoClient.disconnect();
                logger.info("Disconnected from MQTT broker");
            }
            logger.trace("Closing paho-client");
            pahoClient.close(true);
            pahoClient = null;
        }
        catch (MqttException e) {
            logger.warn("Cloudevents message bus did not stop gracefully", e);
        }
    }


    private SSLSocketFactory getSSLSocketFactory(CertificateConfig certificate) throws GeneralSecurityException, IOException {
        try (InputStream keyStoreInputStream = new FileInputStream(certificate.getKeyStorePath())) {
            KeyStore keystore = KeyStore.getInstance(certificate.getKeyStoreType());
            keystore.load(keyStoreInputStream, certificate.getKeyStorePassword().toCharArray());

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keystore, Objects.nonNull(certificate.getKeyPassword()) ? certificate.getKeyPassword().toCharArray() : new char[0]);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keystore);

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagers, null);

            return sslContext.getSocketFactory();
        }
    }


    /**
     * Publishes the message.
     *
     * @param topic the topic to publish on
     * @param content the message to publish
     * @throws MessageBusException if publishing the message fails
     */
    public void publish(String topic, String content) throws MessageBusException {
        if (pahoClient == null || !pahoClient.isConnected()) {
            logger.debug("received data but Cloudevents MQTT connection is closed, trying to connect...");
            start();
        }
        try {
            pahoClient.publish(topic, new MqttMessage(content.getBytes()));
            logger.debug("message published - broker: {} topic: {}, data: {}", config.host(), topic, content);
        }
        catch (MqttException e) {
            throw new MessageBusException(String.format("publishing message on MQTT broker %s failed", config.host()), e);
        }
    }
}
