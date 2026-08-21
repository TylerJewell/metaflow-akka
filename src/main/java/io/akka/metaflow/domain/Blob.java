package io.akka.metaflow.domain;

/**
 * One stored value. {@code packed} is the gzip of the supplied bytes and {@code length} their
 * unpacked length, so a size question is answered without unpacking.
 */
public record Blob(String key, byte[] packed, int length) {}
