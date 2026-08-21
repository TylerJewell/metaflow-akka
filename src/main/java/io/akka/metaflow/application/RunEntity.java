package io.akka.metaflow.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.metaflow.domain.RunCommand;
import io.akka.metaflow.domain.RunEvent;
import io.akka.metaflow.domain.RunState;
import java.util.List;

/**
 * One run of one flow, and the run it descends from where it is a resume (SPEC-001 §3 rule 20).
 *
 * <p>A resumed run holds no copy of its origin's artifacts: what it records is the origin's
 * address, and its tasks reference the same keys.
 */
@Component(id = "run")
public class RunEntity extends EventSourcedEntity<RunState, RunEvent> {

  private final String runId;

  public RunEntity(EventSourcedEntityContext context) {
    this.runId = context.entityId();
  }

  public Effect<Done> create(RunCommand.Create cmd) {
    if (currentState() != null) {
      return effects().error("run '" + runId + "' already exists");
    }
    return effects()
        .persist(new RunEvent.Created(runId, cmd.flowName(), cmd.originRunId()))
        .thenReply(s -> Done.getInstance());
  }

  public Effect<Done> registerTask(String taskId) {
    if (currentState() == null) {
      return effects().error("run '" + runId + "' does not exist");
    }
    if (currentState().taskIds().contains(taskId)) {
      return effects().reply(Done.getInstance());
    }
    return effects()
        .persist(new RunEvent.TaskRegistered(taskId))
        .thenReply(s -> Done.getInstance());
  }

  public ReadOnlyEffect<RunState> get() {
    if (currentState() == null) {
      return effects().error("run '" + runId + "' does not exist");
    }
    return effects().reply(currentState());
  }

  @Override
  public RunState applyEvent(RunEvent event) {
    return switch (event) {
      case RunEvent.Created e -> new RunState(e.runId(), e.flowName(), e.originRunId(), List.of());
      case RunEvent.TaskRegistered e -> currentState().withTask(e.taskId());
    };
  }
}
