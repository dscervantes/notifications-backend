package com.redhat.cloud.notifications.cloudevent.transformers;

import com.redhat.cloud.notifications.events.EventWrapperCloudEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Optional;

@ApplicationScoped
public class CloudEventTransformerFactory {

    /**
     * Returns a CloudEventTransformer if one is registered for the given cloud event type.
     * Currently no transformers are registered (PolicyTriggeredCloudEventTransformer was removed
     * as part of the rhel/policies cleanup). Callers in EventConsumer and ReplayResource will
     * skip the transformation branch until a new transformer is added.
     */
    public Optional<CloudEventTransformer> getTransformerIfSupported(EventWrapperCloudEvent cloudEvent) {
        return Optional.empty();
    }
}
