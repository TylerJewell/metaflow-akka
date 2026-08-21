package io.akka.metaflow.domain;

import akka.javasdk.annotations.TypeName;

public sealed interface RunEvent {

  @TypeName("run-created")
  record Created(String runId, String flowName, String originRunId) implements RunEvent {}

  @TypeName("task-registered")
  record TaskRegistered(String taskId) implements RunEvent {}
}
