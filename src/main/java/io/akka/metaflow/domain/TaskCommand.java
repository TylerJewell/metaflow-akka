package io.akka.metaflow.domain;

import java.util.List;

/** What a caller may ask a task to do (SPEC-001 §3 rules 6-19). */
public sealed interface TaskCommand {

  record RecordArtifacts(int attempt, List<ArtifactRef> artifacts) implements TaskCommand {}

  record CloneFrom(int attempt, String originTaskId, List<ArtifactRef> artifacts)
      implements TaskCommand {}

  /**
   * {@code available} is what the origin holds; {@code names} is the subset asked for. A name the
   * origin does not hold is skipped in silence (rule 19), which is why both are passed together
   * rather than the caller filtering first.
   */
  record PassDown(int attempt, String originTaskId, List<ArtifactRef> available, List<String> names)
      implements TaskCommand {}

  record Finish(int attempt, boolean successful) implements TaskCommand {}

  /** A null attempt means the highest finished one (rule 12). */
  record ReadArtifact(Integer attempt, String name) implements TaskCommand {}

  record Merge(int attempt, List<JoinMerge.Branch> branches, List<String> include, List<String> exclude)
      implements TaskCommand {}
}
