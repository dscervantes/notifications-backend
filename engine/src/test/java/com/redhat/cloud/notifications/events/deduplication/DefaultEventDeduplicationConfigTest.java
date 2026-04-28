package com.redhat.cloud.notifications.events.deduplication;

import com.redhat.cloud.notifications.models.Event;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultEventDeduplicationConfigTest {

    private final DefaultEventDeduplicationConfig deduplicationConfig = new DefaultEventDeduplicationConfig();

    @Test
    void testGetDeduplicationKeyWithNoExternalId() {

        Event event = new Event();

        Optional<String> deduplicationKey = deduplicationConfig.getDeduplicationKey(event);

        assertTrue(deduplicationKey.isEmpty(), "The deduplication key should be empty when externalId is null");
    }

    @Test
    void testGetDeduplicationKeyWithEventIdOnlyReturnsEmpty() {

        UUID eventId = UUID.randomUUID();
        Event event = new Event();
        event.setId(eventId);

        Optional<String> deduplicationKey = deduplicationConfig.getDeduplicationKey(event);

        assertTrue(deduplicationKey.isEmpty(), "The deduplication key should be empty when only event ID is set (no externalId)");
    }

    @Test
    void testGetDeduplicationKeyWithExternalId() {

        UUID externalId = UUID.randomUUID();
        Event event = new Event();
        event.setExternalId(externalId);

        Optional<String> deduplicationKey = deduplicationConfig.getDeduplicationKey(event);

        assertTrue(deduplicationKey.isPresent());
        assertEquals(externalId.toString(), deduplicationKey.get());
    }

    @Test
    void testGetDeduplicationKeyWithDifferentEvents() {

        UUID externalId1 = UUID.randomUUID();
        Event event1 = new Event();
        event1.setExternalId(externalId1);

        Optional<String> deduplicationKey1 = deduplicationConfig.getDeduplicationKey(event1);

        UUID externalId2 = UUID.randomUUID();
        Event event2 = new Event();
        event2.setExternalId(externalId2);

        Optional<String> deduplicationKey2 = deduplicationConfig.getDeduplicationKey(event2);

        assertTrue(deduplicationKey1.isPresent());
        assertTrue(deduplicationKey2.isPresent());
        assertNotEquals(deduplicationKey1.get(), deduplicationKey2.get(), "Different events should have different deduplication keys");
    }

    @Test
    void testGetDeduplicationKeyWithSameData() {

        UUID externalId = UUID.randomUUID();

        Event event1 = new Event();
        event1.setExternalId(externalId);

        Optional<String> deduplicationKey1 = deduplicationConfig.getDeduplicationKey(event1);

        Event event2 = new Event();
        event2.setExternalId(externalId);

        Optional<String> deduplicationKey2 = deduplicationConfig.getDeduplicationKey(event2);

        assertTrue(deduplicationKey1.isPresent());
        assertTrue(deduplicationKey2.isPresent());
        assertEquals(deduplicationKey1.get(), deduplicationKey2.get(), "Events with the same externalId should have the same deduplication key");
    }
}
