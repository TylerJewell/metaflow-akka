package io.akka.metaflow.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.KeyValueEntityTestKit;
import io.akka.metaflow.domain.Blob;
import io.akka.metaflow.domain.ContentKey;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** SPEC-001 §3 rules 3, 4 — store once, load back what was given. */
class BlobEntityTest {

  private static final byte[] BYTES = "the artifact".getBytes(StandardCharsets.UTF_8);
  private static final String KEY = ContentKey.of(BYTES);

  private static KeyValueEntityTestKit<Blob, BlobEntity> kit() {
    return KeyValueEntityTestKit.of(KEY, BlobEntity::new);
  }

  @Test
  void theFirstStoreWritesAndTheSecondDoesNot() {
    var kit = kit();

    var first = kit.method(BlobEntity::store).invoke(BYTES).getReply();
    assertThat(first.stored()).isTrue();
    assertThat(first.key()).isEqualTo(KEY);
    assertThat(first.size()).isEqualTo(BYTES.length);

    var stateAfterFirst = kit.getState();
    var second = kit.method(BlobEntity::store).invoke(BYTES).getReply();

    assertThat(second.stored()).isFalse();
    assertThat(second.key()).isEqualTo(KEY);
    assertThat(kit.getState()).isEqualTo(stateAfterFirst);
  }

  @Test
  void loadReturnsTheBytesThatWereStored() {
    var kit = kit();
    kit.method(BlobEntity::store).invoke(BYTES);

    assertThat(kit.method(BlobEntity::load).invoke().getReply().bytes()).isEqualTo(BYTES);
  }

  @Test
  void whatIsHeldIsThePackedFormNotTheBytes() {
    var kit = kit();
    kit.method(BlobEntity::store).invoke(BYTES);

    assertThat(kit.getState().packed()).isNotEqualTo(BYTES);
    assertThat(ContentKey.unpack(kit.getState().packed())).isEqualTo(BYTES);
    assertThat(kit.getState().length()).isEqualTo(BYTES.length);
  }

  @Test
  void loadingAKeyThatWasNeverStoredIsRefusedByName() {
    var result = kit().method(BlobEntity::load).invoke();
    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains(KEY);
  }

  @Test
  void bytesWhoseDigestIsNotTheEntityAddressAreRefused() {
    var wrongAddress = KeyValueEntityTestKit.of("not-a-digest-of-these-bytes", BlobEntity::new);

    var result = wrongAddress.method(BlobEntity::store).invoke(BYTES);

    assertThat(result.isError()).isTrue();
    assertThat(result.getError()).contains(KEY);
  }
}
