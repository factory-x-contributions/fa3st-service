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
package de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.event.noop;

import de.fraunhofer.iosb.ilt.faaast.service.model.messagebus.EventMessage;
import java.util.Objects;
import java.util.UUID;


/**
 * No-operation event used to analyze the flow of the message bus.
 */
public class NoopEventMessage extends EventMessage {

    private UUID uuid;

    public NoopEventMessage() {
        this.uuid = UUID.randomUUID();
    }


    public UUID getUuid() {
        return uuid;
    }


    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NoopEventMessage that = (NoopEventMessage) o;
        return super.equals(o)
                && Objects.equals(uuid, that.uuid);
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), uuid);
    }


    public static NoopEventMessage.Builder builder() {
        return new NoopEventMessage.Builder();
    }

    public abstract static class AbstractBuilder<T extends NoopEventMessage, B extends NoopEventMessage.AbstractBuilder<T, B>> extends EventMessage.AbstractBuilder<T, B> {

        public B uuid(UUID value) {
            getBuildingInstance().setUuid(value);
            return getSelf();
        }
    }

    public static class Builder extends AbstractBuilder<NoopEventMessage, Builder> {

        @Override
        protected NoopEventMessage.Builder getSelf() {
            return this;
        }


        @Override
        protected NoopEventMessage newBuildingInstance() {
            return new NoopEventMessage();
        }
    }
}
