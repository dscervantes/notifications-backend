-- Event deduplication has been migrated from PostgreSQL to Valkey (in-memory).
-- The event_deduplication table and its cleanup procedure are no longer needed.

DROP PROCEDURE IF EXISTS cleanEventDeduplication();
DROP TABLE IF EXISTS event_deduplication;
