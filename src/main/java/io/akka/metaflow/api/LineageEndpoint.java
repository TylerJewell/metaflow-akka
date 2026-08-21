package io.akka.metaflow.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.client.ComponentClient;
import io.akka.metaflow.application.LineageView;
import java.util.List;

/**
 * Lineage questions: which tasks reference a stored artifact, and what a run holds (SPEC-001 §3
 * rule 27).
 *
 * <p>Answers come from finished attempts across every run, which is what makes an inherited
 * artifact visible from both the run that produced it and the run that resumed it.
 */
@HttpEndpoint("/lineage")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class LineageEndpoint {

  public record TaskRow(
      String taskId,
      String flowName,
      String runId,
      String stepName,
      int attempt,
      boolean successful,
      List<String> artifactKeys,
      List<String> artifactNames) {}

  public record TaskRows(List<TaskRow> tasks) {}

  private final ComponentClient componentClient;

  public LineageEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Get("/artifacts/{key}")
  public TaskRows tasksReferencing(String key) {
    return convert(
        componentClient.forView().method(LineageView::tasksReferencing).invoke(key));
  }

  @Get("/runs/{flow}/{run}")
  public TaskRows tasksOfRun(String flow, String run) {
    return convert(
        componentClient.forView().method(LineageView::tasksOfRun).invoke(flow + "/" + run));
  }

  private static TaskRows convert(LineageView.TaskEntries rows) {
    return new TaskRows(
        rows.tasks().stream()
            .map(
                r ->
                    new TaskRow(
                        r.taskId(),
                        r.flowName(),
                        r.runId(),
                        r.stepName(),
                        r.attempt(),
                        r.successful(),
                        r.artifactKeys(),
                        r.artifactNames()))
            .toList());
  }
}
