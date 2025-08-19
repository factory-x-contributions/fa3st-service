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

import de.fraunhofer.iosb.ilt.faaast.service.config.CertificateConfig;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.MessageBusConfig;
import java.util.Objects;
import java.util.UUID;


/**
 * Configuration class for {@link MessageBusCloudevents}.
 */
public class MessageBusCloudeventsConfig extends MessageBusConfig<MessageBusCloudevents> {

    private static final String DEFAULT_CLIENT_ID = "FA³ST Cloudevents" + UUID.randomUUID();
    private static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "";
    private static final String DEFAULT_CLIENT_KEYSTORE_PATH = "";
    private static final String DEFAULT_HOST = "tcp://localhost:1883";
    private static final String DEFAULT_TOPIC_PREFIX = "events";
    private static final boolean DEFAULT_SLIM_EVENTS = true;

    private String clientId;
    private CertificateConfig clientCertificate;
    private String host;
    private String user;
    private String password;
    private String topicPrefix;
    private boolean slimEvents;

    public MessageBusCloudeventsConfig() {
        this.host = DEFAULT_HOST;
        this.clientCertificate = CertificateConfig.builder()
                .keyStorePath(DEFAULT_CLIENT_KEYSTORE_PATH)
                .keyStorePassword(DEFAULT_CLIENT_KEYSTORE_PASSWORD)
                .build();
        this.clientId = DEFAULT_CLIENT_ID;
        this.topicPrefix = DEFAULT_TOPIC_PREFIX;
        this.user = "user";
        this.password = "password";
        this.slimEvents = DEFAULT_SLIM_EVENTS;
    }


    public String getClientId() {
        return clientId;
    }


    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public String getTopicPrefix() {
        return topicPrefix;
    }


    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }


    public CertificateConfig getClientCertificate() {
        return clientCertificate;
    }


    public void setClientCertificate(CertificateConfig clientCertificate) {
        this.clientCertificate = clientCertificate;
    }


    public String getHost() {
        return host;
    }


    public void setHost(String host) {
        this.host = host;
    }


    public String getPassword() {
        return password;
    }


    public void setPassword(String password) {
        this.password = password;
    }


    public String getUser() {
        return user;
    }


    public void setUser(String user) {
        this.user = user;
    }


    public boolean isSlimEvents() {
        return slimEvents;
    }


    public void setSlimEvents(boolean slimEvents) {
        this.slimEvents = slimEvents;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageBusCloudeventsConfig other = (MessageBusCloudeventsConfig) o;
        return Objects.equals(host, other.host)
                && Objects.equals(clientCertificate, other.clientCertificate)
                && Objects.equals(password, other.password)
                && Objects.equals(clientId, other.clientId)
                && Objects.equals(topicPrefix, other.topicPrefix)
                && Objects.equals(slimEvents, other.slimEvents);
    }


    @Override
    public int hashCode() {
        return Objects.hash(
                host,
                clientCertificate,
                password,
                clientId,
                topicPrefix,
                slimEvents);
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<MessageBusCloudeventsConfig, Builder> {

        @Override
        protected Builder getSelf() {
            return this;
        }


        @Override
        protected MessageBusCloudeventsConfig newBuildingInstance() {
            return new MessageBusCloudeventsConfig();
        }

    }

    private abstract static class AbstractBuilder<T extends MessageBusCloudeventsConfig, B extends AbstractBuilder<T, B>>
            extends MessageBusConfig.AbstractBuilder<MessageBusCloudevents, T, B> {

        public B from(T base) {
            getBuildingInstance().setHost(base.getHost());
            getBuildingInstance().setClientCertificate(base.getClientCertificate());
            getBuildingInstance().setUser(base.getUser());
            getBuildingInstance().setPassword(base.getPassword());
            getBuildingInstance().setClientId(base.getClientId());
            getBuildingInstance().setTopicPrefix(base.getTopicPrefix());
            getBuildingInstance().setSlimEvents(base.isSlimEvents());
            return getSelf();
        }


        public B host(String value) {
            getBuildingInstance().setHost(value);
            return getSelf();
        }


        public B clientCertificate(CertificateConfig value) {
            getBuildingInstance().setClientCertificate(value);
            return getSelf();
        }


        public B user(String value) {
            getBuildingInstance().setUser(value);
            return getSelf();
        }


        public B password(String value) {
            getBuildingInstance().setPassword(value);
            return getSelf();
        }


        public B clientId(String value) {
            getBuildingInstance().setClientId(value);
            return getSelf();
        }


        public B topicPrefix(String value) {
            getBuildingInstance().setTopicPrefix(value);
            return getSelf();
        }


        public B slimEvents(boolean value) {
            getBuildingInstance().setSlimEvents(value);
            return getSelf();
        }

    }
}
