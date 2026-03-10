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
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.MessageBus;
import de.fraunhofer.iosb.ilt.faaast.service.model.IdShortPath;
import de.fraunhofer.iosb.ilt.faaast.service.model.exception.PersistenceException;
import de.fraunhofer.iosb.ilt.faaast.service.model.exception.ResourceNotFoundException;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionId;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionInfo;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.AccessEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationFinishEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationInvokeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementDeleteEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ValueChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.error.ErrorEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.util.EncodingHelper;
import de.fraunhofer.iosb.ilt.faaast.service.util.Ensure;
import de.fraunhofer.iosb.ilt.faaast.service.util.EnvironmentHelper;
import de.fraunhofer.iosb.ilt.faaast.service.util.ReferenceHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Function;
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
    private static final String APPLICATION_JSON = "application/json";

    // Prepare internal message bus to not interfere with internal communications
    private final BlockingQueue<EventMessage> messageQueue;
    private final AtomicBoolean running;
    private final ExecutorService executor;

    private final Map<SubscriptionId, SubscriptionInfo> subscriptions;
    private Function<Reference, Referable> referableSupplier = r -> null;
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
        referableSupplier = reference -> {
            try {
                return EnvironmentHelper.resolve(reference, serviceContext.getAASEnvironment());
            }
            catch (PersistenceException | ResourceNotFoundException persistenceException) {
                // TODO figure out what to do here
                throw new RuntimeException("Resource not found after value change event", persistenceException);

            }
        };

        running.set(false);
    }


    @Override
    public void publish(EventMessage message) throws MessageBusException {
        LOGGER.debug("Publishing {} to {}", message.getClass().getName(), config.getHost());
        try {
            // First, distribute event in internal message bus
            messageQueue.put(message);
            if (isCloudeventMessage(message)) {
                CloudEvent cloudMessage = createCloudevent(message);
                client.publish(config.getTopicPrefix(), objectMapper.writeValueAsString(cloudMessage));
            }
        }
        catch (JsonProcessingException | URISyntaxException publishException) {
            throw new MessageBusException(String.format(PUBLISH_ERROR_MSG, publishException.getClass().getSimpleName(), message.getClass()),
                    publishException);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessageBusException("adding message to internal queue failed", e);
        }
    }


    private boolean isCloudeventMessage(EventMessage m) {
        return !(m instanceof ErrorEventMessage) && !(m instanceof AccessEventMessage);
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


    private CloudEvent createCloudevent(EventMessage message) throws URISyntaxException, JsonProcessingException {
        CloudEventBuilder cloudEventBuilder = createCloudEventBaseBuilder(message.getElement());

        cloudEventBuilder = appendSemanticId(cloudEventBuilder, message);

        cloudEventBuilder.withType(config.getEventTypePrefix().concat(getEventType(message.getClass())));

        if (config.isSlimEvents()) {
            cloudEventBuilder.withData(objectMapper.writeValueAsBytes(referableSupplier.apply(message.getElement())));
        }

        return cloudEventBuilder.build();
    }


    private CloudEventBuilder appendSemanticId(CloudEventBuilder cloudEventBuilder, EventMessage message) {
        // Get referable element (only possible with some EventMessage types)
        Referable element = referableSupplier.apply(message.getElement());

        Optional<String> maybeSemanticId = Optional.ofNullable(getSemanticId(element));
        if (maybeSemanticId.isPresent()) {
            cloudEventBuilder = cloudEventBuilder
                    .withExtension("semanticid", maybeSemanticId.get());
        }

        return cloudEventBuilder;
    }


    private String getSemanticId(Referable referable) {

        if (!(referable instanceof HasSemantics semanticElement) || ReferenceHelper.getRoot(semanticElement.getSemanticId()) == null) {
            return null;
        }

        // If the referable is changed in between the if statement and this one, throw nullpointer
        return Objects.requireNonNull(ReferenceHelper.getRoot(semanticElement.getSemanticId())).getValue();
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
        else if (Objects.equals(messageClass, OperationInvokeEventMessage.class)) {
            return "invoked";
        }
        else if (Objects.equals(messageClass, OperationFinishEventMessage.class)) {
            return "finished";
        }
        else {
            throw new IllegalArgumentException(String.format("EventMessage type not supported: %s", messageClass));
        }
    }


    private CloudEventBuilder createCloudEventBaseBuilder(Reference reference) throws URISyntaxException {
        return CloudEventBuilder
                .v1() // specversion
                .withId(UUID.randomUUID().toString()) // id
                .withSource(getSourceUri(reference)) // source
                .withDataContentType(APPLICATION_JSON) // datacontenttype
                .withDataSchema(new URI(config.getDataSchemaPrefix() + getSpecificElementName(reference))) // dataschema
                .withTime(OffsetDateTime.now()); // time
    }


    private static String getSpecificElementName(Reference reference) {
        KeyTypes effectiveKeyType = Optional.ofNullable(ReferenceHelper.getEffectiveKeyType(reference)).orElseThrow();

        String[] elementNameParts = effectiveKeyType.toString().split("_");
        StringBuilder elementNameBuilder = new StringBuilder();

        for (String elementNamePart: elementNameParts) {
            elementNameBuilder.append(elementNamePart.charAt(0));
            elementNameBuilder.append(elementNamePart.substring(1).toLowerCase());
        }

        return elementNameBuilder.toString();
    }


    private URI getSourceUri(Reference reference) throws URISyntaxException {
        Collection<String> uriString = new ArrayList<>();
        uriString.add(config.getEventCallbackAddress());

        Key root = ReferenceHelper.getRoot(reference);
        if (root == null) {
            throw new IllegalArgumentException("Event reference base element is null");
        }

        String resourceName = switch (root.getType()) {
            case ASSET_ADMINISTRATION_SHELL -> "shells";
            case SUBMODEL -> "submodels";
            case CONCEPT_DESCRIPTION -> "concept-descriptions";
            default -> throw new IllegalArgumentException(String.format("Reference base element type must be one of %s, %s, %s but was %s",
                    ASSET_ADMINISTRATION_SHELL, SUBMODEL, CONCEPT_DESCRIPTION, root.getType()));
        };
        uriString.add(resourceName);

        String resourceIdentifier = root.getValue();
        uriString.add(EncodingHelper.base64UrlEncode(resourceIdentifier));

        if (reference.getKeys().size() > 1) {
            // SubmodelElement
            String idShortPath = IdShortPath.fromReference(reference).toString();
            uriString.add("submodel-elements");
            uriString.add(idShortPath);
        }

        return new URI(String.join("/", uriString));
    }
}
