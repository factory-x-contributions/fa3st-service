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

import de.fraunhofer.iosb.ilt.faaast.service.ServiceContext;
import de.fraunhofer.iosb.ilt.faaast.service.config.CoreConfig;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperation;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.mockito.Mockito;


public abstract class AbstractMessageBusCloudeventsTest<T> {

    private static final ServiceContext SERVICE_CONTEXT = Mockito.mock(ServiceContext.class);
    private static final long DEFAULT_TIMEOUT = 1000;

    private static final Property PROPERTY = new DefaultProperty.Builder()
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
            .collect(Collectors.toList());

    private static final List<EventMessage> ELEMENT_CHANGE_MESSAGES = List.of(
            ELEMENT_CREATE_MESSAGE,
            ELEMENT_UPDATE_MESSAGE,
            ELEMENT_DELETE_MESSAGE);

    private static final List<EventMessage> CHANGE_MESSAGES = Stream.concat(ELEMENT_CHANGE_MESSAGES.stream(), Stream.of(VALUE_CHANGE_MESSAGE))
            .collect(Collectors.toList());

    protected abstract MessageBusCloudeventsConfig getBaseConfig();


    protected abstract T startServer(MessageBusCloudeventsConfig config) throws Exception;


    protected abstract void stopServer(T server);


    private MessageBusInfo startMessageBus(MessageBusCloudeventsConfig config) throws Exception {
        T server = startServer(config);
        MessageBusCloudevents messageBus = new MessageBusCloudevents();
        messageBus.init(CoreConfig.builder().build(), config, SERVICE_CONTEXT);
        messageBus.start();
        return new MessageBusInfo(messageBus, server);
    }


    private void stopMessageBus(MessageBusInfo messageBusInfo) {
        messageBusInfo.messageBus.stop();
        stopServer(messageBusInfo.server);
    }

    private class MessageBusInfo {

        MessageBusCloudevents messageBus;
        T server;

        MessageBusInfo(MessageBusCloudevents messageBus, T server) {
            this.messageBus = messageBus;
            this.server = server;
        }

    }
}
