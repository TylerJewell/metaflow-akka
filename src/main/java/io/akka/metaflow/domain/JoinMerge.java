package io.akka.metaflow.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a join is allowed to take from its incoming branches (SPEC-001 §3 rules 21-26).
 *
 * <p>The comparison is between keys, never between values: two branches agree when they point at
 * the same stored bytes, which is decidable without loading anything.
 */
public final class JoinMerge {

  /** One incoming branch: the task it came from, and what its finished attempt holds. */
  public record Branch(String taskId, Map<String, ArtifactRef> artifacts) {}

  /** The decision itself. It is flattened for the wire by the component that asks for it. */
  public sealed interface Outcome {
    record Merged(List<ArtifactRef> refs) implements Outcome {}

    record Conflict(List<String> names) implements Outcome {}

    record Missing(List<String> names) implements Outcome {}

    record Invalid(String message) implements Outcome {}
  }

  private JoinMerge() {}

  public static Outcome merge(
      List<Branch> branches, Set<String> alreadySet, List<String> include, List<String> exclude) {

    if (!include.isEmpty() && !exclude.isEmpty()) {
      return new Outcome.Invalid("`include` and `exclude` are mutually exclusive in a merge");
    }

    var candidates = new LinkedHashMap<String, ArtifactRef>();
    // A set, not a list: three branches disagreeing on one name report it once
    // (SPEC-001 §4 decision 5), and insertion order is the first-seen order rule 22 asks for.
    var conflicting = new LinkedHashSet<String>();

    for (Branch branch : branches) {
      for (Map.Entry<String, ArtifactRef> entry : branch.artifacts().entrySet()) {
        String name = entry.getKey();
        if (alreadySet.contains(name)) {
          continue;
        }
        if (!include.isEmpty() && !include.contains(name)) {
          continue;
        }
        if (exclude.contains(name)) {
          continue;
        }
        ArtifactRef seen = candidates.putIfAbsent(name, entry.getValue());
        if (seen != null && !seen.key().equals(entry.getValue().key())) {
          conflicting.add(name);
        }
      }
    }

    if (!conflicting.isEmpty()) {
      return new Outcome.Conflict(new ArrayList<>(conflicting));
    }

    var missing = new ArrayList<String>();
    for (String name : include) {
      if (!candidates.containsKey(name) && !alreadySet.contains(name)) {
        missing.add(name);
      }
    }
    if (!missing.isEmpty()) {
      return new Outcome.Missing(missing);
    }

    return new Outcome.Merged(new ArrayList<>(candidates.values()));
  }
}
