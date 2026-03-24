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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementCreateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementDeleteEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ElementUpdateEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.change.ValueChangeEventMessage;
import de.fraunhofer.iosb.ilt.faaast.service.util.EncodingHelper;
import de.fraunhofer.iosb.ilt.faaast.service.util.ReferenceHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd;
import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Referable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.junit.Assert;
import org.junit.Test;


public class CloudEventMapperTest {

    private final String VALUE_CHANGED = "valueChanged";
    private final String UPDATED = "updated";
    private final String DELETED = "deleted";
    private final String CREATED = "created";

    private final String callbackAddress = "https://localhost:12345/api/v3.0";
    private final String dataSchemaPrefix = "https://my-data-schema-prefix/path#";
    private final String eventTypePrefix = "my.prefix.test.";
    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY)
            .registerModule(JsonFormat.getCloudEventJacksonModule());

    private CloudEventMapper getCloudEventMapper(Function<Reference, Referable> referableSupplier) {
        return getCloudEventMapper(false, referableSupplier);
    }


    private CloudEventMapper getCloudEventMapper(boolean slimEvents, Function<Reference, Referable> referableSupplier) {
        return getCloudEventMapper(callbackAddress, dataSchemaPrefix, eventTypePrefix, slimEvents, referableSupplier);
    }


    private CloudEventMapper getCloudEventMapper(String callbackAddress, String dataSchemaPrefix, String eventTypePrefix, boolean slimEvents,
                                                 Function<Reference, Referable> referableSupplier) {
        return new CloudEventMapper(new CloudEventMapperConfig(callbackAddress, dataSchemaPrefix, eventTypePrefix, slimEvents, referableSupplier), objectMapper);
    }


    @Test
    public void testValueChangeMappingValid() throws Exception {
        String submodelId = "hello-world";
        Property property = new DefaultProperty.Builder()
                .idShort("test")
                .semanticId(new DefaultReference.Builder()
                        .keys(new DefaultKey.Builder()
                                .value("my-semantic-id")
                                .build())
                        .build())
                .idShort("ExampleProperty")
                .valueType(DataTypeDefXsd.STRING)
                .value("bar")
                .build();

        CloudEvent expected = expectedFrom(submodelId, property, "Property", VALUE_CHANGED);

        Function<Reference, Referable> referableSupplier = mock(Function.class);
        when(referableSupplier.apply(any())).thenReturn(property);

        CloudEventMapper mapper = getCloudEventMapper(referableSupplier);

        var fastMessage = ValueChangeEventMessage.builder()
                .element(asReference(submodelId, property)).build();

        Assert.assertTrue(mapper.canHandle(fastMessage));

        CloudEvent actual = mapper.createCloudEvent(fastMessage);

        assertCloudEvent(expected, actual);
    }


    @Test
    public void testElementUpdatedMappingValid() throws Exception {
        String submodelId = "hello-world";
        Property property = new DefaultProperty.Builder()
                .idShort("test")
                .semanticId(new DefaultReference.Builder()
                        .keys(new DefaultKey.Builder()
                                .value("my-semantic-id")
                                .build())
                        .build())
                .idShort("ExampleProperty")
                .valueType(DataTypeDefXsd.STRING)
                .value("bar")
                .build();

        CloudEvent expected = expectedFrom(submodelId, property, "Property", UPDATED);

        Function<Reference, Referable> referableSupplier = mock(Function.class);
        when(referableSupplier.apply(any())).thenReturn(property);

        CloudEventMapper mapper = getCloudEventMapper(referableSupplier);

        var fastMessage = ElementUpdateEventMessage.builder()
                .element(asReference(submodelId, property)).build();

        Assert.assertTrue(mapper.canHandle(fastMessage));

        CloudEvent actual = mapper.createCloudEvent(fastMessage);

        assertCloudEvent(expected, actual);
    }


    @Test
    public void testElementDeletedMappingValid() throws Exception {
        String submodelId = "hello-world";
        // Note: omitting semantic id here since technically it is allowed
        Property property = new DefaultProperty.Builder()
                .idShort("test")
                .idShort("ExampleProperty")
                .valueType(DataTypeDefXsd.STRING)
                .value("bar")
                .build();

        CloudEvent expected = expectedFrom(submodelId, property, "Property", DELETED);

        Function<Reference, Referable> referableSupplier = mock(Function.class);
        when(referableSupplier.apply(any())).thenReturn(null);

        CloudEventMapper mapper = getCloudEventMapper(referableSupplier);

        var fastMessage = ElementDeleteEventMessage.builder()
                .element(asReference(submodelId, property)).build();

        Assert.assertTrue(mapper.canHandle(fastMessage));

        CloudEvent actual = mapper.createCloudEvent(fastMessage);

        // We do not send the deleted element in the message
        assertCloudEvent(expected, actual, true);
    }


    @Test
    public void testElementCreatedMappingValid() throws Exception {
        String submodelId = "hello-world";
        Property property = new DefaultProperty.Builder()
                .idShort("test")
                .semanticId(new DefaultReference.Builder()
                        .keys(new DefaultKey.Builder()
                                .value("my-semantic-id")
                                .build())
                        .build())
                .idShort("ExampleProperty")
                .valueType(DataTypeDefXsd.STRING)
                .value("bar")
                .build();

        CloudEvent expected = expectedFrom(submodelId, property, "Property", CREATED);

        Function<Reference, Referable> referableSupplier = mock(Function.class);
        when(referableSupplier.apply(any())).thenReturn(property);

        CloudEventMapper mapper = getCloudEventMapper(referableSupplier);

        var fastMessage = ElementCreateEventMessage.builder()
                .element(asReference(submodelId, property)).build();

        Assert.assertTrue(mapper.canHandle(fastMessage));

        CloudEvent actual = mapper.createCloudEvent(fastMessage);

        assertCloudEvent(expected, actual);
    }


    @Test
    public void testSlimEventsOmitsOnlyData() throws Exception {
        String submodelId = "hello-world";
        Property property = new DefaultProperty.Builder()
                .idShort("test")
                .semanticId(new DefaultReference.Builder()
                        .keys(new DefaultKey.Builder()
                                .value("my-semantic-id")
                                .build())
                        .build())
                .idShort("ExampleProperty")
                .valueType(DataTypeDefXsd.STRING)
                .value("bar")
                .build();

        CloudEvent expected = expectedFrom(submodelId, property, "Property", VALUE_CHANGED);

        Function<Reference, Referable> referableSupplier = mock(Function.class);
        when(referableSupplier.apply(any())).thenReturn(property);

        CloudEventMapper mapper = getCloudEventMapper(true, referableSupplier);

        ValueChangeEventMessage fastMessage = ValueChangeEventMessage.builder()
                .element(asReference(submodelId, property)).build();

        Assert.assertTrue(mapper.canHandle(fastMessage));

        CloudEvent actual = mapper.createCloudEvent(fastMessage);

        assertCloudEvent(expected, actual, true);
    }


    private void assertCloudEvent(CloudEvent expected, CloudEvent actual) {
        assertCloudEvent(expected, actual, false);
    }


    private void assertCloudEvent(CloudEvent expected, CloudEvent actual, boolean isSlim) {
        Assert.assertEquals(expected.getSpecVersion(), actual.getSpecVersion());
        Assert.assertEquals(expected.getSource(), actual.getSource());
        Assert.assertEquals(expected.getType(), actual.getType());
        Assert.assertEquals(expected.getDataSchema(), actual.getDataSchema());
        Assert.assertEquals(expected.getExtension("semanticid"), actual.getExtension("semanticid"));
        Assert.assertEquals(expected.getDataContentType(), actual.getDataContentType());
        if (isSlim) {
            Assert.assertNull(actual.getData());
        }
        else {
            Assert.assertEquals(expected.getData(), actual.getData());

        }
    }


    private CloudEvent expectedFrom(String identifiableId, Referable referable, String dataSchemaSuffix, String eventTypeSuffix) throws JsonProcessingException {
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withSource(URI.create(String.format("%s/submodels/%s/submodel-elements/%s", callbackAddress, EncodingHelper.base64UrlEncode(identifiableId),
                        referable.getIdShort())))
                .withId(UUID.randomUUID().toString())
                .withType(eventTypePrefix + eventTypeSuffix)
                .withDataSchema(URI.create(dataSchemaPrefix + dataSchemaSuffix))
                .withDataContentType("application/json")
                .withData(objectMapper.writeValueAsBytes(referable));

        if (referable instanceof HasSemantics && ((HasSemantics) referable).getSemanticId() != null) {
            builder.withExtension("semanticid", ((HasSemantics) referable).getSemanticId().getKeys().get(0).getValue());
        }

        return builder.build();
    }


    private Reference asReference(String submodelId, Referable referable) {
        return new DefaultReference.Builder()
                .type(ReferenceTypes.MODEL_REFERENCE)
                .keys(List.of(
                        new DefaultKey.Builder().type(KeyTypes.SUBMODEL).value(submodelId).build(),
                        new DefaultKey.Builder().type(ReferenceHelper.toKeyType(referable.getClass())).value(referable.getIdShort()).build()))
                .build();
    }
}
