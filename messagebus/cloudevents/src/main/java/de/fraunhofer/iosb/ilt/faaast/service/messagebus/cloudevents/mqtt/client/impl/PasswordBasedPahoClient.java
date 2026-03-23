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
package de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mqtt.client.impl;

import de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mqtt.client.PahoClient;
import de.fraunhofer.iosb.ilt.faaast.service.messagebus.cloudevents.mqtt.client.config.MqttClientConfig;


/**
 * Implementation of the MQTT client authenticating via password, if available.
 */
public class PasswordBasedPahoClient extends PahoClient {

    public PasswordBasedPahoClient(MqttClientConfig config) {
        super(config);
        if (config.password() != null) {
            setPassword(config.password());
        }
    }
}
