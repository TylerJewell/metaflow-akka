package io.akka.metaflow.api;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import akka.javasdk.http.HttpResponses;
import io.akka.metaflow.application.TaskEntity;
import io.akka.metaflow.domain.ArtifactRef;
import io.akka.metaflow.domain.JoinMerge;
import io.akka.metaflow.domain.TaskCommand;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A task's attempts and its lineage, as an outside caller reaches them (SPEC-001 §3 rules 6-26).
 *
 * <p>A task address is four path segments — flow, run, step, task — and the entity that holds it
 * is addressed by those four joined with slashes, so the address in a lineage answer is the same
 * string a caller would use to fetch the task again.
 */
@HttpEndpoint("/flows/{flow}/runs/{run}/steps/{step}/tasks/{task}")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class TaskEndpoint {

  public record RecordRequest(List<ArtifactRef> artifacts) {}

  public record FinishRequest(boolean successful) {}

  public record CloneRequest(String originTaskId) {}

  public record PassDownRequest(String originTaskId, List<String> names) {}

  public record MergeRequest(
      List<String> branchTaskIds, List<String> include, List<String> exclude) {}

  public record MergeResponse(List<ArtifactRef> merged) {}

  public record MergeRefused(String reason, List<String> artifacts) {}

  public record AttemptView(
      String taskId,
      int attempt,
      boolean successful,
      List<ArtifactRef> artifacts,
      String clonedFrom) {}

  private final ComponentClient componentClient;

  public TaskEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post("/attempts/{attempt}")
  public HttpResponse open(String flow, String run, String step, String task, int attempt) {
    componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::open)
        .invoke(attempt);
    return HttpResponses.created();
  }

  @Post("/attempts/{attempt}/artifacts")
  public HttpResponse record(
      String flow, String run, String step, String task, int attempt, RecordRequest request) {
    componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::record)
        .invoke(new TaskCommand.RecordArtifacts(attempt, request.artifacts()));
    return HttpResponses.ok();
  }

  @Post("/attempts/{attempt}/finish")
  public HttpResponse finish(
      String flow, String run, String step, String task, int attempt, FinishRequest request) {
    componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::finish)
        .invoke(new TaskCommand.Finish(attempt, request.successful()));
    return HttpResponses.ok();
  }

  @Post("/attempts/{attempt}/clone-from")
  public HttpResponse cloneFrom(
      String flow, String run, String step, String task, int attempt, CloneRequest request) {
    var origin = readLatest(request.originTaskId());
    componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::cloneFrom)
        .invoke(new TaskCommand.CloneFrom(attempt, request.originTaskId(), origin.artifacts()));
    return HttpResponses.ok();
  }

  @Post("/attempts/{attempt}/passdown")
  public HttpResponse passDown(
      String flow, String run, String step, String task, int attempt, PassDownRequest request) {
    var origin = readLatest(request.originTaskId());
    componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::passDown)
        .invoke(
            new TaskCommand.PassDown(
                attempt, request.originTaskId(), origin.artifacts(), request.names()));
    return HttpResponses.ok();
  }

  @Post("/attempts/{attempt}/merge")
  public HttpResponse merge(
      String flow, String run, String step, String task, int attempt, MergeRequest request) {
    var branches =
        request.branchTaskIds().stream()
            .map(
                branchTaskId -> {
                  var view = readLatest(branchTaskId);
                  Map<String, ArtifactRef> byName = new LinkedHashMap<>();
                  view.artifacts().forEach(ref -> byName.put(ref.name(), ref));
                  return new JoinMerge.Branch(branchTaskId, byName);
                })
            .toList();

    var outcome =
        componentClient
            .forEventSourcedEntity(taskId(flow, run, step, task))
            .method(TaskEntity::merge)
            .invoke(
                new TaskCommand.Merge(
                    attempt,
                    branches,
                    orEmpty(request.include()),
                    orEmpty(request.exclude())));

    return switch (outcome.outcome()) {
      case "merged" -> HttpResponses.ok(new MergeResponse(outcome.merged()));
      case "conflict", "missing" -> refused(
          new MergeRefused(outcome.message(), outcome.artifacts()));
      default -> throw HttpException.badRequest(outcome.message());
    };
  }

  @Get
  public AttemptView read(String flow, String run, String step, String task) {
    return readLatest(taskId(flow, run, step, task));
  }

  @Get("/attempts/{attempt}")
  public AttemptView readAttempt(
      String flow, String run, String step, String task, int attempt) {
    return view(
        componentClient
            .forEventSourcedEntity(taskId(flow, run, step, task))
            .method(TaskEntity::readAttempt)
            .invoke(attempt));
  }

  @Get("/artifacts/{name}")
  public ArtifactRef artifact(String flow, String run, String step, String task, String name) {
    return componentClient
        .forEventSourcedEntity(taskId(flow, run, step, task))
        .method(TaskEntity::artifact)
        .invoke(new TaskCommand.ReadArtifact(null, name));
  }

  /** A merge that did not resolve: 409, with every artifact it could not settle named. */
  private static HttpResponse refused(MergeRefused refusal) {
    return HttpResponses.of(
        StatusCodes.CONFLICT,
        akka.http.javadsl.model.ContentTypes.APPLICATION_JSON,
        akka.javasdk.JsonSupport.encodeToAkkaByteString(refusal).toArray());
  }

  private AttemptView readLatest(String taskId) {
    return view(
        componentClient.forEventSourcedEntity(taskId).method(TaskEntity::read).invoke());
  }

  private static AttemptView view(TaskEntity.AttemptView from) {
    return new AttemptView(
        from.taskId(), from.attempt(), from.successful(), from.artifacts(), from.clonedFrom());
  }

  private static List<String> orEmpty(List<String> names) {
    return names == null ? List.of() : names;
  }

  private static String taskId(String flow, String run, String step, String task) {
    return flow + "/" + run + "/" + step + "/" + task;
  }
}
