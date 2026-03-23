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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.faaast.service.ServiceContext;
import de.fraunhofer.iosb.ilt.faaast.service.config.CoreConfig;
import de.fraunhofer.iosb.ilt.faaast.service.exception.ConfigurationInitializationException;
import de.fraunhofer.iosb.ilt.faaast.service.exception.MessageBusException;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionId;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.SubscriptionInfo;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.ElementReadEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationFinishEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.OperationInvokeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.access.ValueReadEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementDeleteEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ValueChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.error.ErrorEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.error.ErrorLevel;
import de.fraunhofer.iosb.ilt.faaast.service.model.value.PropertyValue;
import de.fraunhofer.iosb.ilt.faaast.service.model.value.primitive.IntValue;
import de.fraunhofer.iosb.ilt.faaast.service.model.value.primitive.StringValue;
import de.fraunhofer.iosb.ilt.faaast.service.util.EncodingHelper;
import de.fraunhofer.iosb.ilt.faaast.service.util.LambdaExceptionHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.moquette.broker.Server;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultEnvironment;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperation;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodel;
import org.junit.Assert;
import org.junit.Test;


public abstract class AbstractMessageBusCloudeventsTest {

    private static final ServiceContext SERVICE_CONTEXT = mock(ServiceContext.class);
    private static final long DEFAULT_TIMEOUT = 1000;
    private static final String DEFAULT_KEY_STORE_TYPE = "JKS";

    private static final Server SERVER = new Server();

    protected static final String PASSWORD_FILE = "src/test/resources/password_file.conf";
    protected static final String KEYSTORE_PASSWORD = "keystore-password";
    protected static final String KEY_PASSWORD = "key-password";
    protected static final String USER = "user";
    protected static final String USER_PASSWORD_VALID = "user-password";
    protected static final String USER_PASSWORD_INVALID = "user-password-wrong";

    private static final Property PROPERTY = new DefaultProperty.Builder()
            .idShort("test")
            .semanticId(new DefaultReference.Builder().keys(new DefaultKey.Builder().value("my-semantic-id")
                    .build())
                    .build())
            .idShort("ExampleProperty")
            .valueType(DataTypeDefXsd.STRING)
            .value("bar")
            .build();

    private static final Property PARAMETER_IN = new DefaultProperty.Builder()
            .idShort("ParameterIn")
            .valueType(DataTypeDefXsd.STRING)
            .build();

    private static final Property PARAMETER_OUT = new DefaultProperty.Builder()
            .idShort("ParameterOut")
            .valueType(DataTypeDefXsd.STRING)
            .build();

    private static final Operation OPERATION = new DefaultOperation.Builder()
            .idShort("ExampleOperation")
            .inputVariables(new DefaultOperationVariable.Builder()
                    .value(PARAMETER_IN)
                    .build())
            .outputVariables(new DefaultOperationVariable.Builder()
                    .value(PARAMETER_OUT)
                    .build())
            .build();

    private static final Reference OPERATION_REFERENCE = new DefaultReference.Builder()
            .keys(new DefaultKey.Builder()
                    .type(KeyTypes.OPERATION)
                    .value(OPERATION.getIdShort())
                    .build())
            .build();

    private static final Reference PROPERTY_REFERENCE = new DefaultReference.Builder()
            .keys(new DefaultKey.Builder()
                    .type(KeyTypes.PROPERTY)
                    .value(PROPERTY.getIdShort())
                    .build())
            .build();

    private static final ValueChangeEventMessage VALUE_CHANGE_MESSAGE = ValueChangeEventMessage.builder()
            .oldValue(new PropertyValue(new IntValue(100)))
            .oldValue(new PropertyValue(new IntValue(123)))
            .build();

    private static final ElementReadEventMessage ELEMENT_READ_MESSAGE = ElementReadEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .value(PROPERTY)
            .build();

    private static final ValueReadEventMessage VALUE_READ_MESSAGE = ValueReadEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .value(new PropertyValue(new StringValue(PROPERTY.getValue())))
            .build();

    private static final ElementCreateEventMessage ELEMENT_CREATE_MESSAGE = ElementCreateEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .value(PROPERTY)
            .build();

    private static final ElementDeleteEventMessage ELEMENT_DELETE_MESSAGE = ElementDeleteEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .value(PROPERTY)
            .build();

    private static final ElementUpdateEventMessage ELEMENT_UPDATE_MESSAGE = ElementUpdateEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .value(PROPERTY)
            .build();

    private static final OperationInvokeEventMessage OPERATION_INVOKE_MESSAGE = OperationInvokeEventMessage.builder()
            .element(OPERATION_REFERENCE)
            .input(PARAMETER_IN.getIdShort(), new PropertyValue(new StringValue("input")))
            .build();

    private static final OperationFinishEventMessage OPERATION_FINISH_MESSAGE = OperationFinishEventMessage.builder()
            .element(OPERATION_REFERENCE)
            .output(PARAMETER_OUT.getIdShort(), new PropertyValue(new StringValue("result")))
            .build();

    private static final ErrorEventMessage ERROR_MESSAGE = ErrorEventMessage.builder()
            .element(PROPERTY_REFERENCE)
            .level(ErrorLevel.ERROR)
            .build();

    private static final List<EventMessage> ALL_MESSAGES = List.of(
            VALUE_CHANGE_MESSAGE,
            ELEMENT_READ_MESSAGE,
            VALUE_READ_MESSAGE,
            ELEMENT_CREATE_MESSAGE,
            ELEMENT_DELETE_MESSAGE,
            ELEMENT_UPDATE_MESSAGE,
            OPERATION_INVOKE_MESSAGE,
            OPERATION_FINISH_MESSAGE,
            ERROR_MESSAGE);

    private static final List<EventMessage> EXECUTE_MESSAGES = List.of(
            OPERATION_INVOKE_MESSAGE,
            OPERATION_FINISH_MESSAGE);

    private static final List<EventMessage> READ_MESSAGES = List.of(
            ELEMENT_READ_MESSAGE,
            VALUE_READ_MESSAGE);

    private static final List<EventMessage> ACCESS_MESSAGES = Stream.concat(EXECUTE_MESSAGES.stream(), READ_MESSAGES.stream())
            .toList();

    private static final List<EventMessage> ELEMENT_CHANGE_MESSAGES = List.of(
            ELEMENT_CREATE_MESSAGE,
            ELEMENT_UPDATE_MESSAGE,
            ELEMENT_DELETE_MESSAGE);

    private static final List<EventMessage> CHANGE_MESSAGES = Stream.concat(ELEMENT_CHANGE_MESSAGES.stream(), Stream.of(VALUE_CHANGE_MESSAGE))
            .toList();

    private static final String SUBMODEL_ID = EncodingHelper.base64Encode("hello-world");
    private static final CloudEvent VALUE_CHANGE_CLOUD_EVENT;

    static {
        try {
            VALUE_CHANGE_CLOUD_EVENT = CloudEventBuilder.v1()
                    .withSource(URI.create("https://localhost:8080/api/v3.0/submodels/" + SUBMODEL_ID + "/submodel-elements/test"))
                    .withType("io.admin-shell.events.v1.valueChanged")
                    .withDataSchema(URI.create("https://api.swaggerhub.com/domains/Plattform_i40/Part1-MetaModel-Schemas/V3.1.0#/components/schemas/Property"))
                    .withExtension("semanticid", "my-semantic-id")
                    .withDataContentType("application/json")
                    .withData(new ObjectMapper().writeValueAsBytes(PROPERTY))
                    .build();
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract MessageBusCloudeventsConfig getBaseConfig();


    @Test
    public void testCloudEventOnValueChangeEventValid() throws Exception {
        MessageBusInfo info = startMessageBus(getBaseConfig());
        SERVER.startServer();
        when(SERVICE_CONTEXT.getAASEnvironment())
                .thenReturn(new DefaultEnvironment.Builder().submodels(new DefaultSubmodel.Builder().id(SUBMODEL_ID).submodelElements(PROPERTY).build()).build());
        info.messageBus().publish(ValueChangeEventMessage.builder()
                .element(new DefaultReference.Builder()
                        .keys(List.of(
                                new DefaultKey.Builder()
                                        .type(KeyTypes.SUBMODEL)
                                        .value(SUBMODEL_ID)
                                        .build(),
                                new DefaultKey.Builder()
                                        .type(KeyTypes.SUBMODEL_ELEMENT)
                                        .value(PROPERTY.getIdShort())
                                        .build()))
                        .build())
                .build());
    }


    private MessageBusInfo startMessageBus(MessageBusCloudeventsConfig config) throws Exception {
        mockServer(config);
        MessageBusCloudevents messageBus = new MessageBusCloudevents();
        messageBus.init(CoreConfig.builder().build(), config, SERVICE_CONTEXT);
        messageBus.start();
        return new MessageBusInfo(messageBus);
    }


    protected abstract void mockServer(MessageBusCloudeventsConfig config);


    protected abstract void clearServerMocks();


    private void assertConnectionFails(MessageBusCloudeventsConfig config) throws InterruptedException, MessageBusException, ConfigurationInitializationException, IOException {
        MessageBusException expection = Assert.assertThrows(MessageBusException.class, () -> startMessageBus(config));
        Assert.assertEquals("Failed to connect to MQTT server", expection.getMessage());
    }


    private void assertConnectionWorks(MessageBusCloudeventsConfig config) throws Exception {
        MessageBusInfo messageBusInfo = startMessageBus(config);
        stopMessageBus(messageBusInfo);
    }


    private void assertCloudEvent(
                                  MessageBusCloudeventsConfig config,
                                  Class<? extends EventMessage> subscribeTo,
                                  EventMessage toPublish,
                                  CloudEvent expected)
            throws Exception {
        assertCloudEvents(
                config,
                subscribeTo,
                List.of(toPublish),
                Objects.isNull(expected)
                        ? List.of()
                        : List.of(expected));
    }


    private void assertCloudEvent(
                                  MessageBusInfo messageBusInfo,
                                  Class<? extends EventMessage> subscribeTo,
                                  EventMessage toPublish,
                                  CloudEvent expected)
            throws Exception {
        assertCloudEvents(
                messageBusInfo,
                subscribeTo,
                List.of(toPublish),
                Objects.isNull(expected)
                        ? List.of()
                        : List.of(expected));
    }


    private void assertCloudEvents(
                                   MessageBusCloudeventsConfig config,
                                   Class<? extends EventMessage> subscribeTo,
                                   List<EventMessage> toPublish,
                                   List<CloudEvent> expected)
            throws Exception {
        assertCloudEvents(
                config,
                List.of(subscribeTo),
                toPublish,
                Objects.isNull(expected) || expected.isEmpty()
                        ? Map.of()
                        : Map.of(subscribeTo, expected));
    }


    private void assertCloudEvents(
                                   MessageBusInfo messageBusInfo,
                                   Class<? extends EventMessage> subscribeTo,
                                   List<EventMessage> toPublish,
                                   List<CloudEvent> expected)
            throws Exception {
        assertCloudEvents(
                messageBusInfo,
                List.of(subscribeTo),
                toPublish,
                Objects.isNull(expected) || expected.isEmpty()
                        ? Map.of()
                        : Map.of(subscribeTo, expected));
    }


    private void assertCloudEvents(
                                   MessageBusCloudeventsConfig config,
                                   List<Class<? extends EventMessage>> subscribeTo,
                                   List<EventMessage> toPublish,
                                   Map<Class<? extends EventMessage>, List<CloudEvent>> expected)
            throws Exception {
        MessageBusInfo messageBusInfo = startMessageBus(config);
        try {
            assertCloudEvents(messageBusInfo, subscribeTo, toPublish, expected);
        }
        finally {
            messageBusInfo.messageBus.stop();
            clearServerMocks();
        }
    }


    private void assertCloudEvents(
                                   MessageBusInfo messageBusInfo,
                                   List<Class<? extends EventMessage>> subscribeTo,
                                   List<EventMessage> toPublish,
                                   Map<Class<? extends EventMessage>, List<CloudEvent>> expected)
            throws Exception {
        CountDownLatch condition = new CountDownLatch(expected.values().stream().mapToInt(List::size).sum());
        final Map<Class<? extends EventMessage>, List<EventMessage>> actual = Collections.synchronizedMap(new HashMap<>());
        List<SubscriptionId> subscriptions = subscribeTo.stream()
                .map(x -> messageBusInfo.messageBus.subscribe(SubscriptionInfo.create(x, e -> {
                    if (!actual.containsKey(x)) {
                        actual.put(x, Collections.synchronizedList(new ArrayList<>()));
                    }
                    actual.get(x).add(e);
                    condition.countDown();
                })))
                .toList();
        if (Objects.nonNull(toPublish)) {
            toPublish.forEach(LambdaExceptionHelper.rethrowConsumer(messageBusInfo.messageBus::publish));
        }
        condition.await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        subscriptions.forEach(messageBusInfo.messageBus::unsubscribe);
        Assert.assertEquals(Objects.isNull(expected) ? Map.of() : expected, actual);
    }


    private void stopMessageBus(MessageBusInfo messageBusInfo) {
        messageBusInfo.messageBus.stop();
        clearServerMocks();
    }

    private record MessageBusInfo(MessageBusCloudevents messageBus) {}
}
