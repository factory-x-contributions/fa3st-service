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
package de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mapper;

import static org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.ASSET_ADMINISTRATION_SHELL;
import static org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.SUBMODEL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.faaast.service.model.IdShortPath;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationFinishEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationInvokeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementDeleteEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ValueChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.util.EncodingHelper;
import de.fraunhofer.iosb.ilt.faaast.service.util.ReferenceHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Referable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;


public class CloudEventMapper {

    private static final String APPLICATION_JSON = "application/json";

    private final Map<Class<? extends EventMessage>, String> internalToCloudeventMap = Map.of(
            ValueChangeEventMessage.class, "valueChanged",
            ElementCreateEventMessage.class, "created",
            ElementUpdateEventMessage.class, "updated",
            ElementDeleteEventMessage.class, "deleted",
            OperationInvokeEventMessage.class, "invoked",
            OperationFinishEventMessage.class, "finished");
    private final CloudEventMapperConfig config;

    private final ObjectMapper objectMapper;

    public CloudEventMapper(CloudEventMapperConfig config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
    }


    public CloudEvent createCloudevent(EventMessage message) throws URISyntaxException, JsonProcessingException {
        CloudEventBuilder cloudEventBuilder = createCloudEventBaseBuilder(message);

        cloudEventBuilder = appendSemanticId(cloudEventBuilder, message);

        Referable referable = config.referableSupplier().apply(message.getElement());
        if (config.slimEvents() && referable != null) {
            cloudEventBuilder.withData(objectMapper.writeValueAsBytes(referable));
        }

        return cloudEventBuilder.build();
    }


    private CloudEventBuilder appendSemanticId(CloudEventBuilder cloudEventBuilder, EventMessage message) {
        // Get referable element (only possible with some EventMessage types)
        Referable element = config.referableSupplier().apply(message.getElement());

        Optional<String> maybeSemanticId = Optional.ofNullable(element).map(this::getSemanticId);
        if (maybeSemanticId.isPresent()) {
            cloudEventBuilder = cloudEventBuilder
                    .withExtension("semanticid", maybeSemanticId.get());
        }

        return cloudEventBuilder;
    }


    private CloudEventBuilder createCloudEventBaseBuilder(EventMessage message) throws URISyntaxException {
        CloudEventBuilder builder = CloudEventBuilder
                .v1() // specversion
                .withId(UUID.randomUUID().toString()) // id
                .withSource(getSourceUri(message.getElement())) // source
                .withDataContentType(APPLICATION_JSON) // datacontenttype
                .withDataSchema(new URI(config.dataSchemaPrefix() + getSpecificElementName(message.getElement()))) // dataschema
                .withType(config.eventTypePrefix().concat(getEventType(message.getClass()))) // type
                .withTime(OffsetDateTime.now()); // time

        Optional.ofNullable(getSemanticId(config.referableSupplier().apply(message.getElement())))
                .ifPresent(semanticId -> builder.withExtension("semanticid", semanticId));

        return builder;
    }


    private URI getSourceUri(Reference reference) throws URISyntaxException {
        // base
        String uriString = config.eventCallbackAddress().endsWith("/")
                ? config.eventCallbackAddress().substring(0, config.eventCallbackAddress().length() - 1)
                : config.eventCallbackAddress();

        Key root = ReferenceHelper.getRoot(reference);
        if (root == null || root.getValue() == null) {
            throw new IllegalArgumentException(String.format("Event reference malformed: %s", root));
        }

        // identifiable
        uriString = uriString.concat(switch (root.getType()) {
            case ASSET_ADMINISTRATION_SHELL -> "shells";
            case SUBMODEL -> "submodels";
            default -> throw new IllegalArgumentException(String.format("Reference base element type must be %s or %s for cloudevent but was %s",
                    ASSET_ADMINISTRATION_SHELL, SUBMODEL, root.getType()));
        })
                .concat(EncodingHelper.base64UrlEncode(root.getValue()));

        // referable
        if (reference.getKeys().size() > 1) {
            // SubmodelElement
            uriString = uriString.concat("submodel-elements")
                    .concat(IdShortPath.fromReference(reference).toString());
        }

        return new URI(String.join("/", uriString));
    }


    private String getEventType(Class<? extends EventMessage> messageClass) {
        String eventType = internalToCloudeventMap.get(messageClass);

        if (eventType == null) {
            throw new IllegalArgumentException(String.format("EventMessage type not supported: %s", messageClass));
        }
        return eventType;
    }


    private String getSpecificElementName(Reference reference) {
        KeyTypes effectiveKeyType = Optional.ofNullable(ReferenceHelper.getEffectiveKeyType(reference)).orElseThrow();

        String[] elementNameParts = effectiveKeyType.toString().split("_");
        StringBuilder elementNameBuilder = new StringBuilder();

        for (String elementNamePart: elementNameParts) {
            elementNameBuilder.append(elementNamePart.charAt(0));
            elementNameBuilder.append(elementNamePart.substring(1).toLowerCase());
        }

        return elementNameBuilder.toString();
    }


    private String getSemanticId(Referable referable) {
        if (!(referable instanceof HasSemantics semanticElement) || ReferenceHelper.getRoot(semanticElement.getSemanticId()) == null) {
            return null;
        }
        // If the referable is changed in between the if statement and this one, throw nullpointer
        return Optional.ofNullable(ReferenceHelper.getRoot(semanticElement.getSemanticId()))
                .map(Key::getValue)
                .orElse(null);
    }
}
