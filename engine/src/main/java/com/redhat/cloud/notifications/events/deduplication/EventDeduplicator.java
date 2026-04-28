package com.redhat.cloud.notifications.events.deduplication;

import com.redhat.cloud.notifications.config.EngineConfig;
import com.redhat.cloud.notifications.events.ValkeyService;
import com.redhat.cloud.notifications.models.Event;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class EventDeduplicator {

    private static final DefaultEventDeduplicationConfig DEFAULT_DEDUPLICATION_CONFIG = new DefaultEventDeduplicationConfig();
    private static final String SUBSCRIPTION_SERVICES_BUNDLE = "subscription-services";
    private static final String SUBSCRIPTIONS_APP = "subscriptions";

    @Inject
    SubscriptionsDeduplicationConfig subscriptionsDeduplicationConfig;

    @Inject
    EngineConfig engineConfig;

    @Inject
    ValkeyService valkeyService;

    public EventDeduplicationConfig getEventDeduplicationConfig(Event event) {
        return switch (event.getEventType().getApplication().getBundle().getName()) {
            case SUBSCRIPTION_SERVICES_BUNDLE ->
                switch (event.getEventType().getApplication().getName()) {
                    case SUBSCRIPTIONS_APP -> subscriptionsDeduplicationConfig;
                    default -> DEFAULT_DEDUPLICATION_CONFIG;
                };
            default -> DEFAULT_DEDUPLICATION_CONFIG;
        };
    }

    public boolean isNew(Event event) {

        EventDeduplicationConfig eventDeduplicationConfig = getEventDeduplicationConfig(event);
        Optional<String> deduplicationKey = eventDeduplicationConfig.getDeduplicationKey(event);

        // Events are always considered new if no deduplication key is available.
        if (deduplicationKey.isEmpty()) {
            return true;
        }

        // Events are always considered new if Valkey is not available.
        if (!engineConfig.isInMemoryDbEnabled() || !engineConfig.isValkeyEventDeduplicatorEnabled()) {
            return true;
        }

        UUID eventTypeId = event.getEventType().getId();
        LocalDateTime deleteAfter = eventDeduplicationConfig.getDeleteAfter(event);

        return valkeyService.isNewEvent(eventTypeId, deduplicationKey.get(), deleteAfter);
    }
}
