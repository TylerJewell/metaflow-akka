package io.akka.metaflow.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.metaflow.domain.RunCommand;
import io.akka.metaflow.domain.RunEvent;
import io.akka.metaflow.domain.RunState;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rule 20 — a run knows which run it descends from, and which tasks it holds. */
class RunEntityTest {

  private static EventSourcedTestKit<RunState, RunEvent, RunEntity> kit() {
    return EventSourcedTestKit.of("Flow/run-2", RunEntity::new);
  }

  @Test
  void aRunRecordsTheRunItDescendsFrom() {
    var kit = kit();
    kit.method(RunEntity::create).invoke(new RunCommand.Create("Flow", "Flow/run-1"));

    assertThat(kit.method(RunEntity::get).invoke().getReply().originRunId()).isEqualTo("Flow/run-1");
  }

  @Test
  void aRunWithNoOriginSaysSoRatherThanNamingItself() {
    var kit = kit();
    kit.method(RunEntity::create).invoke(new RunCommand.Create("Flow", null));

    assertThat(kit.method(RunEntity::get).invoke().getReply().originRunId()).isNull();
  }

  @Test
  void aRunIsCreatedOnce() {
    var kit = kit();
    kit.method(RunEntity::create).invoke(new RunCommand.Create("Flow", null));

    assertThat(kit.method(RunEntity::create).invoke(new RunCommand.Create("Flow", null)).isError())
        .isTrue();
  }

  @Test
  void tasksAreRegisteredOnceEachAndInOrder() {
    var kit = kit();
    kit.method(RunEntity::create).invoke(new RunCommand.Create("Flow", null));
    kit.method(RunEntity::registerTask).invoke("Flow/run-2/start/t1");
    kit.method(RunEntity::registerTask).invoke("Flow/run-2/end/t2");
    kit.method(RunEntity::registerTask).invoke("Flow/run-2/start/t1");

    assertThat(kit.method(RunEntity::get).invoke().getReply().taskIds())
        .containsExactly("Flow/run-2/start/t1", "Flow/run-2/end/t2");
  }

  @Test
  void aTaskCannotBeRegisteredAgainstARunThatDoesNotExist() {
    assertThat(kit().method(RunEntity::registerTask).invoke("Flow/run-2/start/t1").isError())
        .isTrue();
  }

  @Test
  void readingARunThatDoesNotExistIsRefusedByName() {
    var result = kit().method(RunEntity::get).invoke();
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains("Flow/run-2");
  }
}
