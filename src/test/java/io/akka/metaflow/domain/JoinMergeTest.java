package io.akka.metaflow.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 21-26 — what a join is allowed to merge, and what it must refuse. */
class JoinMergeTest {

  private static ArtifactRef ref(String name, String key) {
    return new ArtifactRef(name, key, 8, "pickle-v4");
  }

  private static JoinMerge.Branch branch(String taskId, ArtifactRef... refs) {
    var byName = new java.util.LinkedHashMap<String, ArtifactRef>();
    for (ArtifactRef r : refs) {
      byName.put(r.name(), r);
    }
    return new JoinMerge.Branch(taskId, byName);
  }

  @Test
  void branchesThatAgreeOnAKeyMergeAndKeepThatKey() {
    var outcome =
        JoinMerge.merge(
            List.of(
                branch("b", ref("x", "key-7"), ref("b_var", "key-1")),
                branch("c", ref("x", "key-7"))),
            Set.of(),
            List.of(),
            List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Merged.class);
    var merged = (JoinMerge.Outcome.Merged) outcome;
    assertThat(merged.refs()).extracting(ArtifactRef::name).containsExactlyInAnyOrder("x", "b_var");
    assertThat(merged.refs().stream().filter(r -> r.name().equals("x")).findFirst().get().key())
        .isEqualTo("key-7");
  }

  @Test
  void branchesThatDisagreeConflictAndEveryConflictingNameIsReportedInFirstSeenOrder() {
    var outcome =
        JoinMerge.merge(
            List.of(
                branch("b", ref("x", "key-7"), ref("q", "key-1")),
                branch("c", ref("x", "key-8"), ref("q", "key-2"))),
            Set.of(),
            List.of(),
            List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Conflict.class);
    assertThat(((JoinMerge.Outcome.Conflict) outcome).names()).containsExactly("x", "q");
  }

  @Test
  void aNameThreeBranchesDisagreeOnIsReportedOnce() {
    var outcome =
        JoinMerge.merge(
            List.of(
                branch("a", ref("v", "key-1")),
                branch("b", ref("v", "key-2")),
                branch("c", ref("v", "key-3"))),
            Set.of(),
            List.of(),
            List.of());

    // SPEC-001 §4 decision 5: the source reports this as [v, v].
    assertThat(((JoinMerge.Outcome.Conflict) outcome).names()).containsExactly("v");
  }

  @Test
  void aNameAlreadySetOnTheJoiningTaskIsNeitherMergedNorAConflict() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("x", "key-7")), branch("c", ref("x", "key-8"))),
            Set.of("x"),
            List.of(),
            List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Merged.class);
    assertThat(((JoinMerge.Outcome.Merged) outcome).refs()).isEmpty();
  }

  @Test
  void excludingAConflictingNameResolvesTheJoin() {
    var outcome =
        JoinMerge.merge(
            List.of(
                branch("b", ref("x", "key-7"), ref("q", "key-1")),
                branch("c", ref("x", "key-7"), ref("q", "key-2"))),
            Set.of(),
            List.of(),
            List.of("q"));

    assertThat(((JoinMerge.Outcome.Merged) outcome).refs())
        .extracting(ArtifactRef::name)
        .containsExactly("x");
  }

  @Test
  void includeNarrowsTheCandidatesToTheNamesGiven() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("x", "key-7"), ref("q", "key-1"))),
            Set.of(),
            List.of("x"),
            List.of());

    assertThat(((JoinMerge.Outcome.Merged) outcome).refs())
        .extracting(ArtifactRef::name)
        .containsExactly("x");
  }

  @Test
  void includeAndExcludeTogetherAreRefused() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("x", "key-7"))), Set.of(), List.of("x"), List.of("q"));

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Invalid.class);
    assertThat(((JoinMerge.Outcome.Invalid) outcome).message()).contains("mutually exclusive");
  }

  @Test
  void includeNamingAnArtifactNoBranchCarriesIsMissing() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("x", "key-7"))), Set.of(), List.of("never_set"), List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Missing.class);
    assertThat(((JoinMerge.Outcome.Missing) outcome).names()).containsExactly("never_set");
  }

  @Test
  void includeNamingAnArtifactTheJoiningTaskAlreadyHoldsIsNotMissing() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("x", "key-7"))), Set.of("already"), List.of("already"), List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Merged.class);
    assertThat(((JoinMerge.Outcome.Merged) outcome).refs()).isEmpty();
  }

  @Test
  void aConflictIsReportedEvenWhenAnotherNameWouldHaveMerged() {
    var outcome =
        JoinMerge.merge(
            List.of(
                branch("b", ref("ok", "key-same"), ref("bad", "key-1")),
                branch("c", ref("ok", "key-same"), ref("bad", "key-2"))),
            Set.of(),
            List.of(),
            List.of());

    assertThat(outcome).isInstanceOf(JoinMerge.Outcome.Conflict.class);
    assertThat(((JoinMerge.Outcome.Conflict) outcome).names()).containsExactly("bad");
  }

  @Test
  void aNameOnlyOneBranchCarriesIsMergedFromThatBranch() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", ref("only_b", "key-1")), branch("c", ref("only_c", "key-2"))),
            Set.of(),
            List.of(),
            List.of());

    assertThat(((JoinMerge.Outcome.Merged) outcome).refs())
        .extracting(ArtifactRef::name)
        .containsExactlyInAnyOrder("only_b", "only_c");
  }

  @Test
  void mergingNoBranchesMergesNothing() {
    var outcome = JoinMerge.merge(List.<JoinMerge.Branch>of(), Set.of(), List.of(), List.of());
    assertThat(((JoinMerge.Outcome.Merged) outcome).refs()).isEmpty();
  }

  @Test
  void mergeCarriesTheIncomingMetadataNotJustTheKey() {
    var outcome =
        JoinMerge.merge(
            List.of(branch("b", new ArtifactRef("x", "key-7", 512, "json"))),
            Set.of(),
            List.of(),
            List.of());

    var only = ((JoinMerge.Outcome.Merged) outcome).refs().get(0);
    assertThat(only.size()).isEqualTo(512);
    assertThat(only.encoding()).isEqualTo("json");
  }

  @Test
  void branchArtifactMapsAreNotMutatedByAMerge() {
    Map<String, ArtifactRef> original = Map.of("x", ref("x", "key-7"));
    var branch = new JoinMerge.Branch("b", original);

    JoinMerge.merge(List.of(branch), Set.of(), List.of(), List.of());

    assertThat(branch.artifacts()).isEqualTo(original);
  }
}
