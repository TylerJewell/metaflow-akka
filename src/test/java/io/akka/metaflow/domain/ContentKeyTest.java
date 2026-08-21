package io.akka.metaflow.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 1, 2, 4, 5 — what a key is, and what it is not affected by. */
class ContentKeyTest {

  @Test
  void keyIsTheSha1OfTheSuppliedBytes() {
    // sha1("hello"), which any other implementation of rule 1 must also produce
    assertThat(ContentKey.of("hello".getBytes(StandardCharsets.UTF_8)))
        .isEqualTo("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
  }

  @Test
  void keyIsOverTheSuppliedBytesNotTheStoredForm() {
    byte[] bytes = "the artifact".getBytes(StandardCharsets.UTF_8);
    byte[] packed = ContentKey.pack(bytes);

    assertThat(packed).isNotEqualTo(bytes);
    assertThat(ContentKey.of(bytes)).isNotEqualTo(ContentKey.of(packed));
    // Both of the above still hold for a key taken over the packed form, so the rule needs the
    // digest itself: sha1("the artifact").
    assertThat(ContentKey.of(bytes)).isEqualTo("df8419382543043a6f6b87551c6138022fa58c8c");
  }

  @Test
  void packingRoundTrips() {
    byte[] bytes = new byte[5000];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i % 251);
    }
    assertThat(ContentKey.unpack(ContentKey.pack(bytes))).isEqualTo(bytes);
    assertThat(ContentKey.pack(bytes).length).isLessThan(bytes.length);
  }

  @Test
  void equalBytesGiveOneKeyWhateverTheyAreCalled() {
    byte[] a = "41".getBytes(StandardCharsets.UTF_8);
    byte[] b = "41".getBytes(StandardCharsets.UTF_8);
    assertThat(ContentKey.of(a)).isEqualTo(ContentKey.of(b));
  }

  @Test
  void keyIsFortyLowerCaseHexCharacters() {
    String key = ContentKey.of(new byte[] {1, 2, 3});
    assertThat(key).hasSize(40).matches("[0-9a-f]{40}");
  }

  @Test
  void bytesPastTheCeilingAreRefusedWithTheCeilingAndTheOfferedSize() {
    byte[] tooBig = new byte[ContentKey.MAX_ARTIFACT_BYTES + 1];
    assertThatThrownBy(() -> ContentKey.requireWithinCeiling(tooBig))
        .isInstanceOf(ArtifactTooLargeException.class)
        .hasMessageContaining(String.valueOf(ContentKey.MAX_ARTIFACT_BYTES))
        .hasMessageContaining(String.valueOf(tooBig.length));

    // The ceiling itself is accepted; the refusal starts one byte past it.
    ContentKey.requireWithinCeiling(new byte[ContentKey.MAX_ARTIFACT_BYTES]);
  }
}
