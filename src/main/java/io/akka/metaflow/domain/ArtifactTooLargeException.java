package io.akka.metaflow.domain;

import akka.javasdk.CommandException;

/** SPEC-001 §3 rule 5 — refused, with the ceiling and the offered size in the message. */
public class ArtifactTooLargeException extends CommandException {
  public ArtifactTooLargeException(String message) {
    super(message);
  }
}
