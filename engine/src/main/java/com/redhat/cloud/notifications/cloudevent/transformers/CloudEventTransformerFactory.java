package com.redhat.cloud.notifications.cloudevent.transformers;

import com.redhat.cloud.notifications.events.EventWrapperCloudEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Optional;

@ApplicationScoped
public class CloudEventTransformerFactory {

    /**
     * Returns a CloudEventTransformer if the transforming is supported
     */
    public Optional<CloudEventTransformer> getTransformerIfSupported(EventWrapperCloudEvent cloudEvent) {
        return Optional.empty();
    }
}
