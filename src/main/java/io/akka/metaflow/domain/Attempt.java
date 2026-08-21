package io.akka.metaflow.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One try at producing a task's artifacts (SPEC-001 §2). The artifact map is insertion-ordered so
 * that a read gives names back in the order they were recorded, which is what the benchmark
 * compares against Metaflow's own manifest ordering.
 */
public record Attempt(
    int number, AttemptState state, Map<String, ArtifactRef> artifacts, Boolean successful) {

  public static Attempt opened(int number) {
    return new Attempt(number, AttemptState.OPEN, new LinkedHashMap<>(), null);
  }

  public Attempt withArtifacts(List<ArtifactRef> refs) {
    var next = new LinkedHashMap<>(artifacts);
    for (ArtifactRef ref : refs) {
      next.put(ref.name(), ref);
    }
    return new Attempt(number, state, next, successful);
  }

  public Attempt finished(boolean wasSuccessful) {
    return new Attempt(number, AttemptState.FINISHED, artifacts, wasSuccessful);
  }

  public boolean isFinished() {
    return state == AttemptState.FINISHED;
  }

  public List<ArtifactRef> artifactList() {
    return new ArrayList<>(artifacts.values());
  }
}
