package com.aegis.surveillance.model;

public record AcceptedFileEvent(
    String eventId,
    String schemaVersion,
    String dataset,
    String filename,
    String storedAt,
    String fingerprint,
    String acceptedAt) {}
