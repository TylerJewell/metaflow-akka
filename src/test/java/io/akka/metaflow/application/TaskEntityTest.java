package io.akka.metaflow.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.metaflow.domain.ArtifactRef;
import io.akka.metaflow.domain.AttemptState;
import io.akka.metaflow.domain.TaskCommand;
import io.akka.metaflow.domain.TaskEvent;
import io.akka.metaflow.domain.TaskState;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 §3 rules 6-20 — the attempt lifecycle, what a reader may see while an attempt is in
 * flight, and what lineage copies.
 *
 * <p>Several of these rules are about what happens *next* time, so the unit under test is a
 * sequence of commands rather than a single one: rule 13 in particular cannot be exhibited by any
 * one call.
 */
class TaskEntityTest {

  private static EventSourcedTestKit<TaskState, TaskEvent, TaskEntity> kit() {
    return EventSourcedTestKit.of("Flow/run-1/start/t1", TaskEntity::new);
  }

  private static ArtifactRef ref(String name, String key) {
    return new ArtifactRef(name, key, 5, "pickle-v4");
  }

  private static void openRecordFinish(
      EventSourcedTestKit<TaskState, TaskEvent, TaskEntity> kit,
      int attempt,
      boolean successful,
      ArtifactRef... refs) {
    kit.method(TaskEntity::open).invoke(attempt);
    kit.method(TaskEntity::record)
        .invoke(new TaskCommand.RecordArtifacts(attempt, List.of(refs)));
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(attempt, successful));
  }

  // --- rules 6, 9: attempt numbering

  @Test
  void attemptsAreDenseAndAscending() {
    var kit = kit();
    assertThat(kit.method(TaskEntity::open).invoke(1).isError()).isTrue();

    kit.method(TaskEntity::open).invoke(0);
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true));

    assertThat(kit.method(TaskEntity::open).invoke(2).isError()).isTrue();
    assertThat(kit.method(TaskEntity::open).invoke(1).isError()).isFalse();
  }

  @Test
  void anAttemptThatIsAlreadyOpenIsNotOpenedAgain() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);
    assertThat(kit.method(TaskEntity::open).invoke(0).isError()).isTrue();
  }

  @Test
  void aFinishedAttemptCannotBeReopened() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));

    var result = kit.method(TaskEntity::open).invoke(0);

    // SPEC-001 §4 decision 2: the source allows this and replaces the manifest.
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains("finished");
    assertThat(kit.method(TaskEntity::read).invoke().getReply().artifacts())
        .containsExactly(ref("x", "key-41"));
  }

  // --- rules 7, 8, 10, 11: writing to an attempt

  @Test
  void nothingIsRecordedAgainstAFinishedAttempt() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));

    var result =
        kit.method(TaskEntity::record)
            .invoke(new TaskCommand.RecordArtifacts(0, List.of(ref("z", "key-1"))));

    assertThat(result.isError()).isTrue();
    assertThat(kit.method(TaskEntity::read).invoke().getReply().artifacts())
        .extracting(ArtifactRef::name)
        .containsExactly("x");
  }

  @Test
  void anAttemptIsFinishedOnlyOnce() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true));

    assertThat(kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true)).isError())
        .isTrue();
  }

  @Test
  void recordingANameTheAttemptAlreadyHoldsReplacesIt() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);
    kit.method(TaskEntity::record)
        .invoke(new TaskCommand.RecordArtifacts(0, List.of(ref("x", "key-41"))));
    kit.method(TaskEntity::record)
        .invoke(new TaskCommand.RecordArtifacts(0, List.of(ref("x", "key-99"))));
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true));

    assertThat(kit.method(TaskEntity::read).invoke().getReply().artifacts())
        .containsExactly(ref("x", "key-99"));
  }

  @Test
  void anUnsuccessfulAttemptIsFinishedLikeAnyOtherAndSaysSo() {
    var kit = kit();
    openRecordFinish(kit, 0, false, ref("_exception", "key-boom"));

    var view = kit.method(TaskEntity::read).invoke().getReply();
    assertThat(view.successful()).isFalse();
    assertThat(view.artifacts()).extracting(ArtifactRef::name).containsExactly("_exception");
  }

  @Test
  void recordingAgainstAnAttemptThatWasNeverOpenedIsRefused() {
    var kit = kit();
    assertThat(
            kit.method(TaskEntity::record)
                .invoke(new TaskCommand.RecordArtifacts(0, List.of(ref("x", "key-1"))))
                .isError())
        .isTrue();
  }

  // --- rules 12-16: what a reader may see, which takes a sequence to exhibit

  @Test
  void aPlainReadGivesTheHighestFinishedAttempt() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));
    openRecordFinish(kit, 1, true, ref("x", "key-99"));

    var view = kit.method(TaskEntity::read).invoke().getReply();
    assertThat(view.attempt()).isEqualTo(1);
    assertThat(view.artifacts()).containsExactly(ref("x", "key-99"));
  }

  @Test
  void anOpenAttemptHidesTheFinishedOneBeneathItFromAPlainRead() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));
    kit.method(TaskEntity::open).invoke(1);

    var result = kit.method(TaskEntity::read).invoke();
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains("1").contains("OPEN");

    // and the moment that attempt finishes, the plain read works again
    kit.method(TaskEntity::record)
        .invoke(new TaskCommand.RecordArtifacts(1, List.of(ref("x", "key-99"))));
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(1, true));
    assertThat(kit.method(TaskEntity::read).invoke().getReply().attempt()).isEqualTo(1);
  }

  @Test
  void theFinishedAttemptUnderneathIsStillReadableByNumber() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));
    kit.method(TaskEntity::open).invoke(1);

    var view = kit.method(TaskEntity::readAttempt).invoke(0).getReply();
    assertThat(view.attempt()).isEqualTo(0);
    assertThat(view.artifacts()).containsExactly(ref("x", "key-41"));
  }

  @Test
  void readingAnOpenAttemptByNumberIsRefusedByName() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);

    var result = kit.method(TaskEntity::readAttempt).invoke(0);

    // SPEC-001 §4 decision 1: the source raises AttributeError from an uninitialised field.
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains("Flow/run-1/start/t1").contains("OPEN");
  }

  @Test
  void anUnknownTaskAndAnUnfinishedOneAreToldApart() {
    var unknown = kit().method(TaskEntity::read).invoke();
    assertThat(unknown.isError()).isTrue();
    assertThat(unknown.getError()).contains("no attempts");

    var started = kit();
    started.method(TaskEntity::open).invoke(0);
    var unfinished = started.method(TaskEntity::read).invoke();
    assertThat(unfinished.isError()).isTrue();
    assertThat(unfinished.getError()).doesNotContain("no attempts");
  }

  @Test
  void readingAnAttemptNumberThatDoesNotExistIsRefused() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));
    assertThat(kit.method(TaskEntity::readAttempt).invoke(3).isError()).isTrue();
  }

  // --- rule 17: reading one artifact

  @Test
  void readingOneArtifactByNameGivesItsReference() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"), ref("y", "key-42"));

    var found =
        kit.method(TaskEntity::artifact)
            .invoke(new TaskCommand.ReadArtifact(null, "y"))
            .getReply();
    assertThat(found.key()).isEqualTo("key-42");
  }

  @Test
  void readingAnArtifactTheAttemptDoesNotHoldIsRefusedByName() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));

    var result = kit.method(TaskEntity::artifact).invoke(new TaskCommand.ReadArtifact(null, "nope"));
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains("nope").contains("Flow/run-1/start/t1");
  }

  // --- rules 18-19: lineage

  @Test
  void cloningCopiesEveryReferenceAndStoresNothing() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);
    kit.method(TaskEntity::cloneFrom)
        .invoke(
            new TaskCommand.CloneFrom(
                0, "Flow/run-0/start/t1", List.of(ref("x", "key-41"), ref("y", "key-42"))));
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true));

    var view = kit.method(TaskEntity::read).invoke().getReply();
    assertThat(view.artifacts()).containsExactly(ref("x", "key-41"), ref("y", "key-42"));
    assertThat(view.clonedFrom()).isEqualTo("Flow/run-0/start/t1");
  }

  @Test
  void passingDownCopiesOnlyTheNamesAskedForAndSkipsWhatTheOriginLacks() {
    var kit = kit();
    kit.method(TaskEntity::open).invoke(0);
    kit.method(TaskEntity::passDown)
        .invoke(
            new TaskCommand.PassDown(
                0,
                "Flow/run-1/b/t2",
                List.of(ref("x", "key-41"), ref("y", "key-42")),
                List.of("x", "not_there")));
    kit.method(TaskEntity::finish).invoke(new TaskCommand.Finish(0, true));

    assertThat(kit.method(TaskEntity::read).invoke().getReply().artifacts())
        .containsExactly(ref("x", "key-41"));
  }

  @Test
  void lineageWritesObeyTheSameFinishedAttemptRuleAsOrdinaryWrites() {
    var kit = kit();
    openRecordFinish(kit, 0, true, ref("x", "key-41"));

    assertThat(
            kit.method(TaskEntity::cloneFrom)
                .invoke(new TaskCommand.CloneFrom(0, "elsewhere", List.of(ref("z", "key-1"))))
                .isError())
        .isTrue();
    assertThat(
            kit.method(TaskEntity::passDown)
                .invoke(
                    new TaskCommand.PassDown(
                        0, "elsewhere", List.of(ref("z", "key-1")), List.of("z")))
                .isError())
        .isTrue();
  }

  @Test
  void stateCarriesEveryAttemptNotOnlyTheLatest() {
    var kit = kit();
    openRecordFinish(kit, 0, false, ref("tries", "key-1"));
    openRecordFinish(kit, 1, true, ref("tries", "key-3"));

    var state = kit.getState();
    assertThat(state.attempts()).hasSize(2);
    assertThat(state.attempts().get(0).state()).isEqualTo(AttemptState.FINISHED);
    assertThat(state.attempts().get(0).successful()).isFalse();
    assertThat(kit.method(TaskEntity::readAttempt).invoke(0).getReply().artifacts())
        .containsExactly(ref("tries", "key-1"));
  }
}
