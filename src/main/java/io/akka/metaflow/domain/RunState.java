package io.akka.metaflow.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * A run, and the run it descends from where it is a resume (SPEC-001 §3 rule 20). {@code
 * originRunId} is null for a run that started from nothing — never the run's own id, so "was this
 * a resume" is answerable without comparing addresses.
 */
public record RunState(String runId, String flowName, String originRunId, List<String> taskIds) {

  public RunState withTask(String taskId) {
    if (taskIds.contains(taskId)) {
      return this;
    }
    var next = new ArrayList<>(taskIds);
    next.add(taskId);
    return new RunState(runId, flowName, originRunId, next);
  }
}
