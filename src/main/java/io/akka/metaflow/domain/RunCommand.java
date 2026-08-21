package io.akka.metaflow.domain;

public sealed interface RunCommand {

  /** {@code originRunId} is null unless this run is a resume of another. */
  record Create(String flowName, String originRunId) implements RunCommand {}
}
