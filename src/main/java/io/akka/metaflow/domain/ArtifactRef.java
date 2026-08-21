package io.akka.metaflow.domain;

/** One named reference to a stored blob, as a task attempt records it (SPEC-001 §2). */
public record ArtifactRef(String name, String key, int size, String encoding) {}
