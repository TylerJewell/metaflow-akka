package io.akka.metaflow.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Query;
import akka.javasdk.view.TableUpdater;
import akka.javasdk.view.View;
import io.akka.metaflow.domain.ArtifactRef;
import java.util.List;

/**
 * Which tasks reference which artifacts (SPEC-001 §3 rule 27).
 *
 * <p>A row is written only when an attempt finishes, so lineage answers describe records that
 * cannot change afterwards. The keys of the finished attempt are held as a list on the row and
 * matched with {@code = ANY}, rather than a row per (task, key) pair.
 */
@Component(id = "lineage")
public class LineageView extends View {

  public record TaskEntry(
      String taskId,
      String flowName,
      String runId,
      String stepName,
      int attempt,
      boolean successful,
      List<String> artifactKeys,
      List<String> artifactNames) {}

  // "rows" is a reserved word in the query language; the collect alias is "tasks".
  public record TaskEntries(List<TaskEntry> tasks) {}

  @Query("SELECT * AS tasks FROM lineage WHERE :key = ANY(artifactKeys)")
  public QueryEffect<TaskEntries> tasksReferencing(String key) {
    return queryResult();
  }

  @Query("SELECT * AS tasks FROM lineage WHERE runId = :runId")
  public QueryEffect<TaskEntries> tasksOfRun(String runId) {
    return queryResult();
  }

  @Consume.FromEventSourcedEntity(TaskEntity.class)
  public static class Updater extends TableUpdater<TaskEntry> {

    public Effect<TaskEntry> onEvent(io.akka.metaflow.domain.TaskEvent event) {
      return switch (event) {
        case io.akka.metaflow.domain.TaskEvent.AttemptFinished finished -> effects()
            .updateRow(row(updateContext().eventSubject().orElseThrow(), finished));
        default -> effects().ignore();
      };
    }

    private static TaskEntry row(
        String taskId, io.akka.metaflow.domain.TaskEvent.AttemptFinished finished) {
      // A task address is flow/run/step/task; anything shorter is not one, and the row says so
      // by leaving the parts it cannot read empty rather than guessing at them.
      String[] parts = taskId.split("/");
      String flow = parts.length > 0 ? parts[0] : "";
      String run = parts.length > 1 ? parts[0] + "/" + parts[1] : "";
      String step = parts.length > 2 ? parts[2] : "";
      return new TaskEntry(
          taskId,
          flow,
          run,
          step,
          finished.attempt(),
          finished.successful(),
          finished.artifacts().stream().map(ArtifactRef::key).toList(),
          finished.artifacts().stream().map(ArtifactRef::name).toList());
    }
  }
}
