package io.akka.metaflow.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.metaflow.domain.ArtifactRef;
import io.akka.metaflow.domain.Attempt;
import io.akka.metaflow.domain.JoinMerge;
import io.akka.metaflow.domain.TaskCommand;
import io.akka.metaflow.domain.TaskEvent;
import io.akka.metaflow.domain.TaskState;
import java.util.List;
import java.util.Optional;

/**
 * One task address — {@code flow/run/step/task} — and every attempt made at it (SPEC-001 §3 rules
 * 6-20).
 *
 * <p>Two rules here are this port's own rather than the source's, and both are refusals: a
 * finished attempt cannot be reopened (§4 decision 2), and an artifact read of an attempt that is
 * still open is refused by name rather than answered (§4 decision 1). Everything the lineage rules
 * claim about a finished attempt — that a clone of it is a copy of what happened, that a key
 * comparison at a join is a comparison of what happened — is a statement about a record that does
 * not change afterwards.
 */
@Component(id = "task")
public class TaskEntity extends EventSourcedEntity<TaskState, TaskEvent> {

  /** What a reader sees. {@code successful} is what the attempt declared, never inferred. */
  public record AttemptView(
      String taskId,
      int attempt,
      boolean successful,
      List<ArtifactRef> artifacts,
      String clonedFrom) {}

  private final String taskId;

  public TaskEntity(akka.javasdk.eventsourcedentity.EventSourcedEntityContext context) {
    this.taskId = context.entityId();
  }

  @Override
  public TaskState emptyState() {
    return TaskState.empty(taskId);
  }

  // --- writing

  public Effect<Done> open(Integer attempt) {
    var existing = currentState().attempt(attempt);
    if (existing.isPresent()) {
      return effects()
          .error(
              "attempt "
                  + attempt
                  + " of '"
                  + taskId
                  + "' is already "
                  + existing.get().state().name().toLowerCase(java.util.Locale.ROOT));
    }
    int expected = currentState().attempts().size();
    if (attempt != expected) {
      return effects()
          .error(
              "attempt "
                  + attempt
                  + " of '"
                  + taskId
                  + "' cannot be opened: the next attempt is "
                  + expected);
    }
    return effects()
        .persist(new TaskEvent.AttemptOpened(taskId, attempt))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> record(TaskCommand.RecordArtifacts cmd) {
    var refused = refuseUnlessOpen(cmd.attempt());
    if (refused.isPresent()) {
      return effects().error(refused.get());
    }
    return effects()
        .persist(new TaskEvent.ArtifactsRecorded(cmd.attempt(), cmd.artifacts()))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> cloneFrom(TaskCommand.CloneFrom cmd) {
    var refused = refuseUnlessOpen(cmd.attempt());
    if (refused.isPresent()) {
      return effects().error(refused.get());
    }
    return effects()
        .persist(
            new TaskEvent.ArtifactsCloned(cmd.attempt(), cmd.originTaskId(), cmd.artifacts()))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> passDown(TaskCommand.PassDown cmd) {
    var refused = refuseUnlessOpen(cmd.attempt());
    if (refused.isPresent()) {
      return effects().error(refused.get());
    }
    // A name the origin does not hold is skipped without comment (rule 19).
    var taken =
        cmd.available().stream().filter(ref -> cmd.names().contains(ref.name())).toList();
    return effects()
        .persist(new TaskEvent.ArtifactsRecorded(cmd.attempt(), taken))
        .thenReply(s -> Done.getInstance());
  }

  /**
   * A merge, flattened for the wire: the decision itself is {@link JoinMerge.Outcome}, but a reply
   * carrying a sealed type cannot be decoded back into that type by the caller, so the case is
   * named in a field.
   */
  public record MergeResult(
      String outcome, List<ArtifactRef> merged, List<String> artifacts, String message) {

    static MergeResult of(JoinMerge.Outcome outcome) {
      return switch (outcome) {
        case JoinMerge.Outcome.Merged m -> new MergeResult("merged", m.refs(), List.of(), null);
        case JoinMerge.Outcome.Conflict c ->
            new MergeResult("conflict", List.of(), c.names(), "conflicting values");
        case JoinMerge.Outcome.Missing m ->
            new MergeResult("missing", List.of(), m.names(), "not present on any branch");
        case JoinMerge.Outcome.Invalid i ->
            new MergeResult("invalid", List.of(), List.of(), i.message());
      };
    }
  }

  public Effect<MergeResult> merge(TaskCommand.Merge cmd) {
    var refused = refuseUnlessOpen(cmd.attempt());
    if (refused.isPresent()) {
      return effects().error(refused.get());
    }
    var attempt = currentState().attempt(cmd.attempt()).orElseThrow();
    var outcome =
        JoinMerge.merge(cmd.branches(), attempt.artifacts().keySet(), cmd.include(), cmd.exclude());

    // A merge that did not resolve leaves the attempt exactly as it was (rule 22).
    if (!(outcome instanceof JoinMerge.Outcome.Merged merged) || merged.refs().isEmpty()) {
      return effects().reply(MergeResult.of(outcome));
    }
    return effects()
        .persist(new TaskEvent.ArtifactsRecorded(cmd.attempt(), merged.refs()))
        .thenReply(s -> MergeResult.of(outcome));
  }

  public Effect<Done> finish(TaskCommand.Finish cmd) {
    var refused = refuseUnlessOpen(cmd.attempt());
    if (refused.isPresent()) {
      return effects().error(refused.get());
    }
    var attempt = currentState().attempt(cmd.attempt()).orElseThrow();
    return effects()
        .persist(
            new TaskEvent.AttemptFinished(cmd.attempt(), cmd.successful(), attempt.artifactList()))
        .thenReply(s -> Done.getInstance());
  }

  // --- reading

  public ReadOnlyEffect<AttemptView> read() {
    if (currentState().attempts().isEmpty()) {
      return effects().error("task '" + taskId + "' has no attempts");
    }
    var highest = currentState().highest().orElseThrow();
    if (!highest.isFinished()) {
      return effects()
          .error(
              "attempt "
                  + highest.number()
                  + " of '"
                  + taskId
                  + "' is "
                  + highest.state()
                  + "; name an earlier attempt to read it");
    }
    return effects().reply(view(highest));
  }

  public ReadOnlyEffect<AttemptView> readAttempt(Integer attempt) {
    var found = currentState().attempt(attempt);
    if (found.isEmpty()) {
      return effects().error("task '" + taskId + "' has no attempt " + attempt);
    }
    if (!found.get().isFinished()) {
      return effects()
          .error("attempt " + attempt + " of '" + taskId + "' is " + found.get().state());
    }
    return effects().reply(view(found.get()));
  }

  public ReadOnlyEffect<ArtifactRef> artifact(TaskCommand.ReadArtifact cmd) {
    Optional<Attempt> found =
        cmd.attempt() == null ? currentState().highestFinished() : currentState().attempt(cmd.attempt());
    if (found.isEmpty()) {
      return effects().error("task '" + taskId + "' has no finished attempt to read");
    }
    if (!found.get().isFinished()) {
      return effects()
          .error("attempt " + found.get().number() + " of '" + taskId + "' is " + found.get().state());
    }
    ArtifactRef ref = found.get().artifacts().get(cmd.name());
    if (ref == null) {
      return effects().error("task '" + taskId + "' holds no artifact named '" + cmd.name() + "'");
    }
    return effects().reply(ref);
  }

  // --- events

  /**
   * Applying an event only moves what the event carries into state. It never decides anything and
   * never fails: an event is a thing that already happened, and a replay that can throw is an
   * entity that cannot be recovered. Whether the attempt could be written at all was settled by
   * the command handler that persisted this.
   */
  @Override
  public TaskState applyEvent(TaskEvent event) {
    return switch (event) {
      case TaskEvent.AttemptOpened e -> currentState().with(Attempt.opened(e.attempt()));
      case TaskEvent.ArtifactsRecorded e ->
          currentState().with(attemptOrOpened(e.attempt()).withArtifacts(e.artifacts()));
      case TaskEvent.ArtifactsCloned e ->
          currentState()
              .with(attemptOrOpened(e.attempt()).withArtifacts(e.artifacts()))
              .clonedFrom(e.originTaskId());
      case TaskEvent.AttemptFinished e ->
          currentState().with(attemptOrOpened(e.attempt()).finished(e.successful()));
    };
  }

  private Attempt attemptOrOpened(int number) {
    return currentState().attempt(number).orElseGet(() -> Attempt.opened(number));
  }

  private Optional<String> refuseUnlessOpen(int attempt) {
    var found = currentState().attempt(attempt);
    if (found.isEmpty()) {
      return Optional.of("attempt " + attempt + " of '" + taskId + "' has not been opened");
    }
    if (found.get().isFinished()) {
      return Optional.of("attempt " + attempt + " of '" + taskId + "' is finished");
    }
    return Optional.empty();
  }

  private AttemptView view(Attempt attempt) {
    return new AttemptView(
        taskId,
        attempt.number(),
        Boolean.TRUE.equals(attempt.successful()),
        attempt.artifactList(),
        currentState().clonedFrom());
  }
}
