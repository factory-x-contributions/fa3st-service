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
