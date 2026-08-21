package io.akka.metaflow.domain;

import akka.javasdk.annotations.TypeName;
import java.util.List;

/** What a task records about itself. The target requires this to be a sealed interface. */
public sealed interface TaskEvent {

  @TypeName("attempt-opened")
  record AttemptOpened(String taskId, int attempt) implements TaskEvent {}

  @TypeName("artifacts-recorded")
  record ArtifactsRecorded(int attempt, List<ArtifactRef> artifacts) implements TaskEvent {}

  @TypeName("artifacts-cloned")
  record ArtifactsCloned(int attempt, String originTaskId, List<ArtifactRef> artifacts)
      implements TaskEvent {}

  @TypeName("attempt-finished")
  record AttemptFinished(int attempt, boolean successful, List<ArtifactRef> artifacts)
      implements TaskEvent {}
}
