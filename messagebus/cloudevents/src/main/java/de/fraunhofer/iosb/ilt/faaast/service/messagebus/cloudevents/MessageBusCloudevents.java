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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.fraunhofer.iosb.ilt.faaast.service.ServiceContext;
import de.fraunhofer.iosb.ilt.faaast.service.config.CoreConfig;
import de.fraunhofer.iosb.ilt.faaast.service.dataformat.json.JsonEventDeserializer;
import de.fraunhofer.iosb.ilt.faaast.service.dataformat.json.JsonEventSerializer;
import de.fraunhofer.iosb.ilt.faaast.service.exception.ConfigurationInitializationException;
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.MessageBus;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionId;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionInfo;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.util.Ensure;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;

import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.eclipse.digitaltwin.aas4j.v3.model.ModelType.ASSET_ADMINISTRATION_SHELL;


/**
 * MessageBusCloudevents: Implements the external MessageBus interface and publishes/dispatchesEventMessages.
 */
public class MessageBusCloudevents implements MessageBus<MessageBusCloudeventsConfig> {

    private static final String EVENT_TYPE_PREFIX = "io.admin-shell.events.v1.";
    private static final String DATA_SCHEMA_PREFIX = "https://api.swaggerhub.com/domains/Plattform_i40/Part1-MetaModel-Schemas/V3.1" +
            ".0#/components/schemas/";
    private static final String FAAAST_PREFIX = "/api/v3.0";

    private final Map<SubscriptionId, SubscriptionInfo> subscriptions;
    private final JsonEventSerializer serializer;
    private final JsonEventDeserializer deserializer;
    private MessageBusCloudeventsConfig config;
    private PahoClient client;
    private ObjectMapper objectMapper;

    public MessageBusCloudevents() {
        subscriptions = new ConcurrentHashMap<>();
        serializer = new JsonEventSerializer();
        deserializer = new JsonEventDeserializer();
    }


    @Override
    public MessageBusCloudeventsConfig asConfig() {
        return config;
    }


    @Override
    public void init(CoreConfig coreConfig, MessageBusCloudeventsConfig config, ServiceContext serviceContext) throws ConfigurationInitializationException {
        this.config = config;
        client = new PahoClient(config);
        this.objectMapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.registerModule(JsonFormat.getCloudEventJacksonModule());
    }


    @Override
    public void publish(EventMessage message) throws MessageBusException {
        try {
            CloudEvent cloudMessage = createCloudevent(message);
            if (cloudMessage != null) {
                client.publish(config.getTopicPrefix(), objectMapper.writeValueAsString(cloudMessage));
            }
        } catch (Exception e) {
            throw new MessageBusException(String.format("Error publishing event via Cloudevents MQTT message bus for message type {s}",
                    message.getClass()), e);
        }
    }


    private CloudEvent createCloudevent(EventMessage message) throws URISyntaxException, JsonProcessingException {
        boolean isAas = message.getElement().toString().contains(ASSET_ADMINISTRATION_SHELL.name());

        if (message instanceof ElementCreateEventMessage createMessage) {
            return isAas ? AasElementCreated(createMessage) : SubmodelElementCreated(createMessage);
        } else if (message instanceof ElementUpdateEventMessage updateMessage) {
            return isAas ? AasValueChanged(updateMessage) : SubmodelValueChanged(updateMessage);
        } else {
            return null;
        }
    }


    private CloudEvent AasElementCreated(ElementCreateEventMessage message) throws URISyntaxException, JsonProcessingException {
        return createAasCloudEventBuilder(message)
                .withType(EVENT_TYPE_PREFIX + "AASElementCreated")
                .build();
    }


    private CloudEvent AasValueChanged(ElementUpdateEventMessage message) throws URISyntaxException, JsonProcessingException {
        return createAasCloudEventBuilder(message)
                .withType(EVENT_TYPE_PREFIX + "AASValueChanged")
                .build();
    }


    private CloudEventBuilder createAasCloudEventBuilder(ElementChangeEventMessage message) throws JsonProcessingException, URISyntaxException {
        var sourceUri = new URI(config.getEventCallbackAddress() + FAAAST_PREFIX + "/shells/" +
                base64Encode(message.getElement().getKeys().get(0).getValue()));

        return createCloudEventBuilder(message)
                .withSource(sourceUri)
                .withDataSchema(new URI(DATA_SCHEMA_PREFIX + "AssetAdministrationShell"));
    }


    private CloudEvent SubmodelValueChanged(ElementUpdateEventMessage message) throws URISyntaxException, JsonProcessingException {
        var properties = message.getElement().getKeys();
        URI source = new URI(config.getEventCallbackAddress() + FAAAST_PREFIX + "/submodels/" +
                base64Encode(properties.get(0).getValue()));

        if (properties.size() > 1) {
            source = source.resolve("submodel-elements");
        }

        for (int i = 1; i < properties.size(); i++) {
            source = source.resolve(properties.get(i).getValue());
        }

        return createCloudEventBuilder(message)
                .withType(EVENT_TYPE_PREFIX + "SubmodelValueChanged")
                .withSource(source)
                .withDataSchema(new URI(DATA_SCHEMA_PREFIX + "Submodel"))
                .build();
    }


    private CloudEvent SubmodelElementCreated(ElementCreateEventMessage message) throws JsonProcessingException, URISyntaxException {
        return createSubmodelCloudEventBuilder(message)
                .withType(EVENT_TYPE_PREFIX + "SubmodelElementCreated")
                .build();
    }


    private CloudEventBuilder createSubmodelCloudEventBuilder(ElementChangeEventMessage message) throws JsonProcessingException, URISyntaxException {
        var elementPath = message.getElement().getKeys();
        StringBuilder source = new StringBuilder(config.getEventCallbackAddress() + FAAAST_PREFIX + "/submodels/" +
                base64Encode(elementPath.get(0).getValue()));

        if (elementPath.size() > 1) {
            source.append("/submodel-elements/");
            for (int i = 1; i < elementPath.size(); i++) {
                source.append(".").append(elementPath.get(i).getValue());
            }
        }

        return createCloudEventBuilder(message)
                .withSource(new URI(source.toString()))
                .withDataSchema(new URI(DATA_SCHEMA_PREFIX + "Submodel"));
    }


    private CloudEventBuilder createCloudEventBuilder(ElementChangeEventMessage message) throws JsonProcessingException {
        var builder = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withTime(OffsetDateTime.now())
                .withDataContentType("application/json");

        if (!config.isSlimEvents()) {
            builder.withData(objectMapper.writeValueAsString(message.getValue()).getBytes());
        }

        return builder;
    }


    private String base64Encode(String toEncode) {
        return Base64.getEncoder().encodeToString(toEncode.getBytes(StandardCharsets.UTF_8));
    }


    @Override
    public void start() throws MessageBusException {
        client.start();
    }


    @Override
    public void stop() {
        client.stop();
    }


    @Override
    public SubscriptionId subscribe(SubscriptionInfo subscriptionInfo) {
        Ensure.requireNonNull(subscriptionInfo, "subscriptionInfo must be non-null");
        subscriptionInfo.getSubscribedEvents()
                .forEach(x -> determineEvents((Class<? extends EventMessage>) x).stream()
                        .forEach(e -> client.subscribe(config.getTopicPrefix() + e.getSimpleName(), (t, message) -> {
                            EventMessage event = deserializer.read(message.toString(), e);
                            if (subscriptionInfo.getFilter().test(event.getElement())) {
                                subscriptionInfo.getHandler().accept(event);
                            }
                        })));

        SubscriptionId subscriptionId = new SubscriptionId();
        subscriptions.put(subscriptionId, subscriptionInfo);
        return subscriptionId;
    }


    private List<Class<EventMessage>> determineEvents(Class<? extends EventMessage> messageType) {
        try (ScanResult scanResult = new ClassGraph().acceptPackages("de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event")
                .enableClassInfo().scan()) {
            if (Modifier.isAbstract(messageType.getModifiers())) {
                return scanResult
                        .getSubclasses(messageType.getName())
                        .filter(x -> !x.isAbstract())
                        .loadClasses(EventMessage.class);
            } else {
                List<Class<EventMessage>> list = new ArrayList<>();
                list.add((Class<EventMessage>) messageType);
                return list;
            }
        }
    }


    @Override
    public void unsubscribe(SubscriptionId id) {
        SubscriptionInfo info = subscriptions.get(id);
        Ensure.requireNonNull(info.getSubscribedEvents(), "subscriptionInfo must be non-null");
        subscriptions.get(id).getSubscribedEvents().stream().forEach(a -> //find all events for given abstract or event
                determineEvents((Class<? extends EventMessage>) a).stream().forEach(e -> //unsubscribe from all events
                        client.unsubscribe(config.getTopicPrefix() + e.getSimpleName())));
        subscriptions.remove(id);
    }
}
