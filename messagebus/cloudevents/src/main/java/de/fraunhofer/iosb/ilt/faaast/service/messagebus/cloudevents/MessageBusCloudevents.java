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

import static org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.ASSET_ADMINISTRATION_SHELL;
import static org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.CONCEPT_DESCRIPTION;
import static org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.SUBMODEL;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.fraunhofer.iosb.ilt.faaast.service.ServiceContext;
import de.fraunhofer.iosb.ilt.faaast.service.config.CoreConfig;
import de.fraunhofer.iosb.ilt.faaast.service.dataformat.SerializationException;
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.MessageBus;
import de.fraunhofer.iosb.ilt.faaast.service.model.IdShortPath;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionId;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionInfo;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.AccessEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.ElementReadEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationFinishEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationInvokeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.ReadEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.ValueReadEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementDeleteEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ValueChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.error.ErrorEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.util.Ensure;
import de.fraunhofer.iosb.ilt.faaast.service.util.ReferenceHelper;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Referable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MessageBusCloudevents: Implements the external MessageBus interface and publishes/dispatchesEventMessages.
 *
 * <p>
 * Also implements the internal messagebus functionality to support internal components relying on Events.
 */
public class MessageBusCloudevents implements MessageBus<MessageBusCloudeventsConfig> {

    public static final String PUBLISH_ERROR_MSG = "%s publishing event via Cloudevents MQTT message bus for message type %s";

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBusCloudevents.class);

    // Prepare internal message bus to not interfere with internal communications
    private final BlockingQueue<EventMessage> messageQueue;
    private final AtomicBoolean running;
    private final ExecutorService executor;

    private final Map<SubscriptionId, SubscriptionInfo> subscriptions;
    private MessageBusCloudeventsConfig config;
    private PahoClient client;
    private ObjectMapper objectMapper;

    public MessageBusCloudevents() {
        running = new AtomicBoolean(false);
        messageQueue = new LinkedBlockingDeque<>();

        subscriptions = new ConcurrentHashMap<>();
        executor = Executors.newSingleThreadExecutor();
    }


    @Override
    public void start() throws MessageBusException {
        client.start();

        executor.submit(this::run);
    }


    @Override
    public void stop() {
        client.stop();

        running.set(false);
        executor.shutdown();

        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.error("interrupted while waiting for shutdown.", e);
            Thread.currentThread().interrupt();
        }
    }


    @Override
    public SubscriptionId subscribe(SubscriptionInfo subscriptionInfo) {
        // Note: this only pertains to local subscriptions. Subscriptions to the cloudevents MQTT broker are handled by that component
        Ensure.requireNonNull(subscriptionInfo, "subscriptionInfo must be non-null");
        SubscriptionId subscriptionId = new SubscriptionId();
        subscriptions.put(subscriptionId, subscriptionInfo);
        return subscriptionId;
    }


    @Override
    public void unsubscribe(SubscriptionId id) {
        // Note: this only pertains to local subscriptions. Subscriptions to the cloudevents MQTT broker are handled by that component
        subscriptions.remove(id);
    }


    @Override
    public MessageBusCloudeventsConfig asConfig() {
        return config;
    }


    @Override
    public void init(CoreConfig coreConfig, MessageBusCloudeventsConfig config, ServiceContext serviceContext) {
        this.config = config;
        client = new PahoClient(config);
        this.objectMapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.registerModule(JsonFormat.getCloudEventJacksonModule());

        running.set(false);
    }


    @Override
    public void publish(EventMessage message) throws MessageBusException {
        LOGGER.debug("Publishing {} to {}", message.getClass().getName(), config.getHost());
        try {
            CloudEvent cloudMessage = createCloudevent(message);
            client.publish(config.getTopicPrefix(), objectMapper.writeValueAsString(cloudMessage));
            // Also distribute event in internal message bus
            messageQueue.put(message);
        }
        catch (JsonProcessingException | URISyntaxException | SerializationException publishException) {
            throw new MessageBusException(String.format(PUBLISH_ERROR_MSG, publishException.getClass().getSimpleName(), message.getClass()),
                    publishException);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessageBusException("adding message to internal queue failed", e);
        }
    }


    private void run() {
        running.set(true);
        try {
            while (running.get()) {
                EventMessage message = messageQueue.take();
                Class<? extends EventMessage> messageType = message.getClass();
                for (SubscriptionInfo subscription: subscriptions.values()) {
                    if (subscription.getSubscribedEvents().stream().anyMatch(x -> x.isAssignableFrom(messageType))
                            && subscription.getFilter().test(message.getElement())) {
                        subscription.getHandler().accept(message);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private CloudEvent createCloudevent(EventMessage message) throws URISyntaxException, SerializationException, JsonProcessingException {
        CloudEventBuilder cloudEventBuilder = createCloudEventBaseBuilder(message.getElement());

        cloudEventBuilder.withType(config.getEventTypePrefix().concat(getEventType(message.getClass())));

        cloudEventBuilder = appendEventTypeSpecific(cloudEventBuilder, message);

        if (message.getElement().getKeys().size() == 1) {
            cloudEventBuilder = appendSemanticId(cloudEventBuilder, message);
        }

        return cloudEventBuilder.build();
    }


    private CloudEventBuilder appendSemanticId(CloudEventBuilder cloudEventBuilder, EventMessage message) {
        // Get referable element (only possible with some EventMessage types)
        Referable element;
        if (message instanceof ElementChangeEventMessage elementChangeEventMessage) {
            element = elementChangeEventMessage.getValue();
        }
        else if (message instanceof ElementReadEventMessage elementReadEventMessage) {
            element = elementReadEventMessage.getValue();
        }
        else {
            return cloudEventBuilder;
        }

        Optional<String> maybeSemanticId = getSemanticIdFirstKeyValue(element);
        if (maybeSemanticId.isPresent()) {
            cloudEventBuilder = cloudEventBuilder
                    .withExtension("semanticid", maybeSemanticId.get());
        }

        return cloudEventBuilder;
    }


    private Optional<String> getSemanticIdFirstKeyValue(Referable referable) {

        if (!(referable instanceof HasSemantics semanticElement)) {
            return Optional.empty();

        }
        Key semanticId = ReferenceHelper.getRoot(semanticElement.getSemanticId());

        if (semanticId == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(semanticId.getValue());
    }


    private CloudEventBuilder appendEventTypeSpecific(CloudEventBuilder builder, EventMessage message) throws JsonProcessingException {
        if (message instanceof ChangeEventMessage changeEventMessage) {
            return appendChange(builder, changeEventMessage);
        }
        else if (message instanceof AccessEventMessage accessEventMessage) {
            return appendAccess(builder, accessEventMessage);
        }
        else if (message instanceof ErrorEventMessage errorEventMessage) {
            return appendError(builder, errorEventMessage);
        }
        throw new IllegalArgumentException(String.format("EventType unknown: %s", message.getClass().getSimpleName()));
    }


    private CloudEventBuilder appendError(CloudEventBuilder builder, ErrorEventMessage errorEventMessage) throws JsonProcessingException {
        return withData(builder, errorEventMessage);
    }


    private CloudEventBuilder appendAccess(CloudEventBuilder builder, AccessEventMessage accessEventMessage) throws JsonProcessingException {
        if (accessEventMessage instanceof ReadEventMessage<?>) {
            return withData(builder, accessEventMessage);
        }

        return builder;
    }


    private CloudEventBuilder appendChange(CloudEventBuilder builder, ChangeEventMessage changeEventMessage) throws JsonProcessingException {
        if (changeEventMessage instanceof ValueChangeEventMessage || changeEventMessage instanceof ElementChangeEventMessage) {
            return withData(builder, changeEventMessage);
        }

        return builder;
    }


    private String getEventType(Class<? extends EventMessage> messageClass) {
        if (Objects.equals(messageClass, ValueChangeEventMessage.class)) {
            return "valueChanged";
        }
        else if (Objects.equals(messageClass, ElementCreateEventMessage.class)) {
            return "created";
        }
        else if (Objects.equals(messageClass, ElementUpdateEventMessage.class)) {
            return "updated";
        }
        else if (Objects.equals(messageClass, ElementDeleteEventMessage.class)) {
            return "deleted";
        }
        else if (Objects.equals(messageClass, ElementReadEventMessage.class)) {
            return "read";
        }
        else if (Objects.equals(messageClass, ValueReadEventMessage.class)) {
            return "valueRead";
        }
        else if (Objects.equals(messageClass, OperationInvokeEventMessage.class)) {
            return "invoked";
        }
        else if (Objects.equals(messageClass, OperationFinishEventMessage.class)) {
            return "finished";
        }
        else if (Objects.equals(messageClass, ErrorEventMessage.class)) {
            return "error";
        }
        else {
            throw new IllegalArgumentException(String.format("EventMessage type not recognized: %s", messageClass));
        }
    }


    private static String getSpecificElementName(Reference reference) {
        List<Key> referenceKeys = reference.getKeys();

        if (referenceKeys.isEmpty()) {
            throw new IllegalArgumentException(String.format("Event reference contains no keys: %s", reference));
        }

        KeyTypes elementKeyType = referenceKeys
                .get(referenceKeys.size() - 1) // Get most specific key
                .getType(); // Get type from enum

        String[] elementNameParts = elementKeyType.toString().split("_");
        StringBuilder elementNameBuilder = new StringBuilder();

        for (String elementNamePart: elementNameParts) {
            elementNameBuilder.append(elementNamePart.charAt(0));
            elementNameBuilder.append(elementNamePart.substring(1).toLowerCase());
        }

        return elementNameBuilder.toString();
    }


    private CloudEventBuilder withData(CloudEventBuilder builder, EventMessage eventMessage) throws JsonProcessingException {
        if (config.isSlimEvents()) {
            return builder;
        }

        if (eventMessage instanceof ElementChangeEventMessage messageWithReferable) {
            return builder.withData(objectMapper.writeValueAsBytes(messageWithReferable.getValue()));
        }

        if (eventMessage instanceof ValueChangeEventMessage valueChangeEventMessage) {
            return builder.withData(valueChangeEventMessage.getNewValue().toString().getBytes(StandardCharsets.UTF_8));
        }

        if (eventMessage instanceof ErrorEventMessage errorEventMessage) {
            return builder.withData(errorEventMessage.getMessage().getBytes(StandardCharsets.UTF_8));
        }

        if (eventMessage instanceof ReadEventMessage<?> readEventMessage) {
            return builder.withData(objectMapper.writeValueAsBytes(readEventMessage.getValue()));
        }

        return builder;
    }


    private CloudEventBuilder createCloudEventBaseBuilder(Reference reference) throws URISyntaxException {
        return CloudEventBuilder
                .v1() // specversion
                .withId(UUID.randomUUID().toString()) // id
                .withSource(getSourceUri(reference)) // source
                .withDataContentType("application/json") // datacontenttype
                .withDataSchema(new URI(config.getDataSchemaPrefix() + getSpecificElementName(reference))) // dataschema
                .withTime(OffsetDateTime.now()); // time
    }


    private URI getSourceUri(Reference reference) throws URISyntaxException {
        Collection<String> uriString = new ArrayList<>();
        uriString.add(config.getEventCallbackAddress());

        String resourceName = switch (reference.getKeys().get(0).getType()) {
            case ASSET_ADMINISTRATION_SHELL -> "shells";
            case SUBMODEL -> "submodels";
            case CONCEPT_DESCRIPTION -> "concept-descriptions";
            default -> throw new IllegalArgumentException(String.format("Reference base element type must be one of %s, %s, %s",
                    ASSET_ADMINISTRATION_SHELL, SUBMODEL, CONCEPT_DESCRIPTION));
        };
        uriString.add(resourceName);

        String resourceIdentifier = reference.getKeys().get(0).getValue();
        uriString.add(base64Encode(resourceIdentifier));

        if (reference.getKeys().size() > 1) {
            // SubmodelElement
            String idShortPath = IdShortPath.fromReference(reference).toString();
            uriString.add("submodel-elements");
            uriString.add(idShortPath);
        }

        return new URI(String.join("/", uriString));
    }


    private String base64Encode(String toEncode) {
        return Base64.getEncoder().encodeToString(toEncode.getBytes(StandardCharsets.UTF_8));
    }


    private List<Class<EventMessage>> determineEvents(Class<? extends EventMessage> messageType) {
        try (ScanResult scanResult = new ClassGraph().acceptPackages("de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event")
                .enableClassInfo().scan()) {
            if (Modifier.isAbstract(messageType.getModifiers())) {
                return scanResult
                        .getSubclasses(messageType.getName())
                        .filter(x -> !x.isAbstract())
                        .loadClasses(EventMessage.class);
            }
            else {
                List<Class<EventMessage>> list = new ArrayList<>();
                list.add((Class<EventMessage>) messageType);
                return list;
            }
        }
    }
}
