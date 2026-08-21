package io.akka.metaflow.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.testkit.TestKitSupport;
import io.akka.metaflow.domain.ArtifactRef;
import io.akka.metaflow.domain.ContentKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Every rule in SPEC-001 §3 as an outside caller reaches it: this test starts the runtime and
 * drives the HTTP surface, so the wire shapes and the view are exercised, not only the components
 * behind them.
 */
public class RuntimeBackedHttpSurfaceTest extends TestKitSupport {

  private String run() {
    return "run-" + UUID.randomUUID();
  }

  private ArtifactEndpoint.StoredArtifact store(byte[] bytes) {
    return httpClient
        .POST("/artifacts")
        .withRequestBody(ContentTypes.APPLICATION_OCTET_STREAM, bytes)
        .responseBodyAs(ArtifactEndpoint.StoredArtifact.class)
        .invoke()
        .body();
  }

  private String taskPath(String run, String step, String task) {
    return "/flows/Flow/runs/" + run + "/steps/" + step + "/tasks/" + task;
  }

  private void openAttempt(String path, int attempt) {
    httpClient.POST(path + "/attempts/" + attempt).invoke();
  }

  private void recordArtifacts(String path, int attempt, List<ArtifactRef> refs) {
    httpClient
        .POST(path + "/attempts/" + attempt + "/artifacts")
        .withRequestBody(new TaskEndpoint.RecordRequest(refs))
        .invoke();
  }

  private void finish(String path, int attempt, boolean successful) {
    httpClient
        .POST(path + "/attempts/" + attempt + "/finish")
        .withRequestBody(new TaskEndpoint.FinishRequest(successful))
        .invoke();
  }

  private TaskEndpoint.AttemptView read(String path) {
    return httpClient.GET(path).responseBodyAs(TaskEndpoint.AttemptView.class).invoke().body();
  }

  // --- artifacts

  @Test
  void storingTheSameBytesTwiceStoresOnceAndReadsBackWhatWasGiven() {
    byte[] bytes = ("payload " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

    var first = store(bytes);
    var second = store(bytes);

    assertThat(first.key()).isEqualTo(ContentKey.of(bytes));
    assertThat(first.stored()).isTrue();
    assertThat(second.stored()).isFalse();
    assertThat(second.key()).isEqualTo(first.key());

    var loaded = httpClient.GET("/artifacts/" + first.key()).invoke().body();
    assertThat(loaded.toArray()).isEqualTo(bytes);
  }

  @Test
  void anArtifactPastTheCeilingIsRefusedWithTheCeilingAndTheOfferedSize() {
    byte[] tooBig = new byte[ContentKey.MAX_ARTIFACT_BYTES + 1];

    var response =
        httpClient
            .POST("/artifacts")
            .withRequestBody(ContentTypes.APPLICATION_OCTET_STREAM, tooBig)
            .invoke();

    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.PAYLOAD_TOO_LARGE);
    assertThat(response.body().utf8String())
        .contains(String.valueOf(ContentKey.MAX_ARTIFACT_BYTES))
        .contains(String.valueOf(tooBig.length));
  }

  @Test
  void anArtifactKeyThatWasNeverStoredIsNotFound() {
    var response = httpClient.GET("/artifacts/" + "0".repeat(40)).invoke();
    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.NOT_FOUND);
  }

  // --- attempts, in sequence

  @Test
  void anOpenAttemptHidesTheFinishedOneBeneathItUntilItFinishes() {
    String run = run();
    String path = taskPath(run, "start", "t1");
    var x41 = new ArtifactRef("x", "key-41", 5, "pickle-v4");
    var x99 = new ArtifactRef("x", "key-99", 5, "pickle-v4");

    openAttempt(path, 0);
    recordArtifacts(path, 0, List.of(x41));
    finish(path, 0, true);
    assertThat(read(path).artifacts()).containsExactly(x41);

    openAttempt(path, 1);
    assertThat(httpClient.GET(path).invoke().httpResponse().status())
        .isEqualTo(StatusCodes.BAD_REQUEST);

    var byNumber =
        httpClient
            .GET(path + "/attempts/0")
            .responseBodyAs(TaskEndpoint.AttemptView.class)
            .invoke()
            .body();
    assertThat(byNumber.artifacts()).containsExactly(x41);

    recordArtifacts(path, 1, List.of(x99));
    finish(path, 1, true);
    assertThat(read(path).artifacts()).containsExactly(x99);
    assertThat(read(path).attempt()).isEqualTo(1);
  }

  @Test
  void aFinishedAttemptIsClosedToFurtherWrites() {
    String path = taskPath(run(), "start", "t1");
    openAttempt(path, 0);
    finish(path, 0, true);

    var response =
        httpClient
            .POST(path + "/attempts/0/artifacts")
            .withRequestBody(
                new TaskEndpoint.RecordRequest(
                    List.of(new ArtifactRef("z", "key-1", 1, "pickle-v4"))))
            .invoke();

    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.BAD_REQUEST);
  }

  @Test
  void anUnsuccessfulAttemptIsFinishedAndReadableAndSaysItFailed() {
    String path = taskPath(run(), "start", "t1");
    var boom = new ArtifactRef("_exception", "key-boom", 9, "pickle-v4");

    openAttempt(path, 0);
    recordArtifacts(path, 0, List.of(boom));
    finish(path, 0, false);

    var view = read(path);
    assertThat(view.successful()).isFalse();
    assertThat(view.artifacts()).containsExactly(boom);
  }

  // --- lineage

  @Test
  void aResumedRunReusesItsOriginKeysWithoutStoringAnythingAgain() {
    String originRun = run();
    String resumedRun = run();
    byte[] bytes = ("resume " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
    var stored = store(bytes);
    var ref = new ArtifactRef("data", stored.key(), bytes.length, "pickle-v4");

    String originPath = taskPath(originRun, "start", "t1");
    openAttempt(originPath, 0);
    recordArtifacts(originPath, 0, List.of(ref));
    finish(originPath, 0, true);

    httpClient
        .POST("/flows/Flow/runs/" + resumedRun)
        .withRequestBody(new RunEndpoint.NewRun("Flow/" + originRun))
        .invoke();

    String resumedPath = taskPath(resumedRun, "start", "t1");
    openAttempt(resumedPath, 0);
    httpClient
        .POST(resumedPath + "/attempts/0/clone-from")
        .withRequestBody(new TaskEndpoint.CloneRequest("Flow/" + originRun + "/start/t1"))
        .invoke();
    finish(resumedPath, 0, true);

    assertThat(read(resumedPath).artifacts()).containsExactly(ref);
    // the key was inherited, so a store of the same bytes still reports nothing new stored
    assertThat(store(bytes).stored()).isFalse();

    var runState =
        httpClient
            .GET("/flows/Flow/runs/" + resumedRun)
            .responseBodyAs(RunEndpoint.RunView.class)
            .invoke()
            .body();
    assertThat(runState.originRunId()).isEqualTo("Flow/" + originRun);
  }

  @Test
  void passingDownCopiesOnlyTheNamesAskedFor() {
    String run = run();
    var x = new ArtifactRef("x", "key-x", 5, "pickle-v4");
    var y = new ArtifactRef("y", "key-y", 5, "pickle-v4");

    String from = taskPath(run, "b", "t2");
    openAttempt(from, 0);
    recordArtifacts(from, 0, List.of(x, y));
    finish(from, 0, true);

    String to = taskPath(run, "join", "t4");
    openAttempt(to, 0);
    httpClient
        .POST(to + "/attempts/0/passdown")
        .withRequestBody(
            new TaskEndpoint.PassDownRequest(
                "Flow/" + run + "/b/t2", List.of("x", "not_there")))
        .invoke();
    finish(to, 0, true);

    assertThat(read(to).artifacts()).containsExactly(x);
  }

  // --- joins

  @Test
  void aJoinOverBranchesThatAgreeMergesAndKeepsTheIncomingKey() {
    String run = run();
    var x = new ArtifactRef("x", "key-7", 5, "pickle-v4");
    var bOnly = new ArtifactRef("b_var", "key-1", 5, "pickle-v4");

    branch(run, "b", "t2", List.of(x, bOnly));
    branch(run, "c", "t3", List.of(x));

    String join = taskPath(run, "join", "t4");
    openAttempt(join, 0);
    var response = merge(run, join, List.of("b/t2", "c/t3"), List.of(), List.of());
    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.OK);
    finish(join, 0, true);

    assertThat(read(join).artifacts()).containsExactlyInAnyOrder(x, bOnly);
  }

  @Test
  void aJoinOverBranchesThatDisagreeIsRefusedAndNamesEveryConflict() {
    String run = run();
    branch(
        run,
        "b",
        "t2",
        List.of(
            new ArtifactRef("x", "key-7", 5, "pickle-v4"),
            new ArtifactRef("q", "key-1", 5, "pickle-v4")));
    branch(
        run,
        "c",
        "t3",
        List.of(
            new ArtifactRef("x", "key-8", 5, "pickle-v4"),
            new ArtifactRef("q", "key-2", 5, "pickle-v4")));

    String join = taskPath(run, "join", "t4");
    openAttempt(join, 0);
    var response = merge(run, join, List.of("b/t2", "c/t3"), List.of(), List.of());

    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.CONFLICT);
    assertThat(response.body().utf8String()).contains("x").contains("q");

    // nothing was set: the join's own attempt is still empty
    finish(join, 0, true);
    assertThat(read(join).artifacts()).isEmpty();
  }

  @Test
  void anArtifactAlreadySetOnTheJoiningTaskIsNeitherMergedNorAConflict() {
    String run = run();
    branch(run, "b", "t2", List.of(new ArtifactRef("x", "key-7", 5, "pickle-v4")));
    branch(run, "c", "t3", List.of(new ArtifactRef("x", "key-8", 5, "pickle-v4")));

    String join = taskPath(run, "join", "t4");
    openAttempt(join, 0);
    var chosen = new ArtifactRef("x", "key-8", 5, "pickle-v4");
    recordArtifacts(join, 0, List.of(chosen));

    var response = merge(run, join, List.of("b/t2", "c/t3"), List.of(), List.of());
    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.OK);
    finish(join, 0, true);

    assertThat(read(join).artifacts()).containsExactly(chosen);
  }

  @Test
  void includeAndExcludeTogetherAreRefused() {
    String run = run();
    branch(run, "b", "t2", List.of(new ArtifactRef("x", "key-7", 5, "pickle-v4")));

    String join = taskPath(run, "join", "t4");
    openAttempt(join, 0);
    var response = merge(run, join, List.of("b/t2"), List.of("x"), List.of("q"));

    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.BAD_REQUEST);
  }

  @Test
  void includeNamingAnArtifactNoBranchCarriesIsRefused() {
    String run = run();
    branch(run, "b", "t2", List.of(new ArtifactRef("x", "key-7", 5, "pickle-v4")));

    String join = taskPath(run, "join", "t4");
    openAttempt(join, 0);
    var response = merge(run, join, List.of("b/t2"), List.of("never_set"), List.of());

    assertThat(response.httpResponse().status()).isEqualTo(StatusCodes.CONFLICT);
    assertThat(response.body().utf8String()).contains("never_set");
  }

  // --- lineage questions

  @Test
  void oneKeyFindsEveryTaskThatReferencesIt() {
    String run = run();
    String shared = "key-shared-" + UUID.randomUUID();
    var sharedRef = new ArtifactRef("shared", shared, 5, "pickle-v4");
    var ownRef = new ArtifactRef("own", "key-own-" + UUID.randomUUID(), 5, "pickle-v4");

    branch(run, "b", "t2", List.of(sharedRef));
    branch(run, "c", "t3", List.of(sharedRef, ownRef));

    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              var found =
                  httpClient
                      .GET("/lineage/artifacts/" + shared)
                      .responseBodyAs(LineageEndpoint.TaskRows.class)
                      .invoke()
                      .body();
              assertThat(found.tasks())
                  .extracting(LineageEndpoint.TaskRow::taskId)
                  .containsExactlyInAnyOrder(
                      "Flow/" + run + "/b/t2", "Flow/" + run + "/c/t3");
            });

    var onlyOne =
        httpClient
            .GET("/lineage/artifacts/" + ownRef.key())
            .responseBodyAs(LineageEndpoint.TaskRows.class)
            .invoke()
            .body();
    assertThat(onlyOne.tasks()).hasSize(1);
  }

  @Test
  void aRunListsTheTasksItHolds() {
    String run = run();
    branch(run, "b", "t2", List.of(new ArtifactRef("x", "key-x", 5, "pickle-v4")));
    branch(run, "c", "t3", List.of(new ArtifactRef("y", "key-y", 5, "pickle-v4")));

    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              var rows =
                  httpClient
                      .GET("/lineage/runs/Flow/" + run)
                      .responseBodyAs(LineageEndpoint.TaskRows.class)
                      .invoke()
                      .body();
              assertThat(rows.tasks())
                  .extracting(LineageEndpoint.TaskRow::taskId)
                  .containsExactlyInAnyOrder(
                      "Flow/" + run + "/b/t2", "Flow/" + run + "/c/t3");
            });
  }

  private void branch(String run, String step, String task, List<ArtifactRef> refs) {
    String path = taskPath(run, step, task);
    openAttempt(path, 0);
    recordArtifacts(path, 0, refs);
    finish(path, 0, true);
  }

  private akka.javasdk.http.StrictResponse<akka.util.ByteString> merge(
      String run, String joinPath, List<String> branchSuffixes, List<String> include, List<String> exclude) {
    var branchTaskIds = branchSuffixes.stream().map(s -> "Flow/" + run + "/" + s).toList();
    return httpClient
        .POST(joinPath + "/attempts/0/merge")
        .withRequestBody(new TaskEndpoint.MergeRequest(branchTaskIds, include, exclude))
        .invoke();
  }
}
