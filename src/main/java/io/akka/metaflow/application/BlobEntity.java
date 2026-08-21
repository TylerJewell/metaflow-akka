package io.akka.metaflow.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.keyvalueentity.KeyValueEntity;
import io.akka.metaflow.domain.Blob;
import io.akka.metaflow.domain.ContentKey;

/**
 * One stored artifact, addressed by the digest of its own bytes (SPEC-001 §3 rules 1-4).
 *
 * <p>Because the address is the digest, "have I already got this?" is answered by whether this
 * entity has state at all — there is no lookup to race against, and a repeat store writes nothing.
 */
@Component(id = "blob")
public class BlobEntity extends KeyValueEntity<Blob> {

  public record StoreResult(String key, boolean stored, int size) {}

  public record BlobPayload(String key, byte[] bytes) {}

  public Effect<StoreResult> store(byte[] bytes) {
    String key = commandContext().entityId();
    String actual = ContentKey.of(bytes);
    if (!actual.equals(key)) {
      return effects()
          .error("bytes offered at '" + key + "' hash to '" + actual + "'");
    }
    if (currentState() != null) {
      return effects().reply(new StoreResult(key, false, currentState().length()));
    }
    return effects()
        .updateState(new Blob(key, ContentKey.pack(bytes), bytes.length))
        .thenReply(new StoreResult(key, true, bytes.length));
  }

  public ReadOnlyEffect<BlobPayload> load() {
    if (currentState() == null) {
      return effects().error("no artifact stored at '" + commandContext().entityId() + "'");
    }
    return effects()
        .reply(
            new BlobPayload(currentState().key(), ContentKey.unpack(currentState().packed())));
  }
}
