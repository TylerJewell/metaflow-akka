package io.akka.metaflow.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpResponses;
import io.akka.metaflow.application.RunEntity;
import io.akka.metaflow.domain.RunCommand;
import java.util.List;

/** Runs, and which run a resumed run descends from (SPEC-001 §3 rule 20). */
@HttpEndpoint("/flows/{flow}/runs/{run}")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class RunEndpoint {

  /** {@code originRunId} is absent unless this run is a resume of another. */
  public record NewRun(String originRunId) {}

  public record RunView(
      String runId, String flowName, String originRunId, List<String> taskIds) {}

  private final ComponentClient componentClient;

  public RunEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post
  public HttpResponse create(String flow, String run, NewRun request) {
    componentClient
        .forEventSourcedEntity(runId(flow, run))
        .method(RunEntity::create)
        .invoke(new RunCommand.Create(flow, request == null ? null : request.originRunId()));
    return HttpResponses.created();
  }

  @Post("/tasks")
  public HttpResponse registerTask(String flow, String run, String taskId) {
    componentClient
        .forEventSourcedEntity(runId(flow, run))
        .method(RunEntity::registerTask)
        .invoke(taskId);
    return HttpResponses.ok();
  }

  @Get
  public RunView get(String flow, String run) {
    var state =
        componentClient.forEventSourcedEntity(runId(flow, run)).method(RunEntity::get).invoke();
    return new RunView(state.runId(), state.flowName(), state.originRunId(), state.taskIds());
  }

  private static String runId(String flow, String run) {
    return flow + "/" + run;
  }
}
