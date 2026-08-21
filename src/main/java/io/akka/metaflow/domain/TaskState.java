package io.akka.metaflow.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A task address and every attempt made at it (SPEC-001 §2). Attempts are held in order and never
 * removed: rule 14 lets a caller ask for an earlier one by number long after a later one exists.
 */
public record TaskState(String taskId, List<Attempt> attempts, String clonedFrom) {

  public static TaskState empty(String taskId) {
    return new TaskState(taskId, List.of(), null);
  }

  public Optional<Attempt> attempt(int number) {
    return attempts.stream().filter(a -> a.number() == number).findFirst();
  }

  public Optional<Attempt> highest() {
    return attempts.isEmpty() ? Optional.empty() : Optional.of(attempts.get(attempts.size() - 1));
  }

  public Optional<Attempt> highestFinished() {
    for (int i = attempts.size() - 1; i >= 0; i--) {
      if (attempts.get(i).isFinished()) {
        return Optional.of(attempts.get(i));
      }
    }
    return Optional.empty();
  }

  public TaskState with(Attempt attempt) {
    var next = new ArrayList<>(attempts);
    for (int i = 0; i < next.size(); i++) {
      if (next.get(i).number() == attempt.number()) {
        next.set(i, attempt);
        return new TaskState(taskId, next, clonedFrom);
      }
    }
    next.add(attempt);
    return new TaskState(taskId, next, clonedFrom);
  }

  public TaskState clonedFrom(String originTaskId) {
    return new TaskState(taskId, attempts, originTaskId);
  }
}
