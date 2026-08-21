package io.akka.metaflow.domain;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The address of an artifact, and the form it is kept in (SPEC-001 §3 rules 1-5).
 *
 * <p>The key is the digest of the bytes as supplied, never of the stored form: a reader who has
 * the bytes can work out the key without asking anything, which is what makes a key computed here
 * and a key computed by Metaflow over the same bytes the same string.
 */
public final class ContentKey {

  /**
   * The largest artifact this port accepts (SPEC-001 §4 decision 3). Measured, not guessed: the
   * target refuses an entity command above 1,048,479 bytes on the wire, which is 786,285 raw
   * bytes; the margin below that covers a 40-character key in the address.
   */
  public static final int MAX_ARTIFACT_BYTES = 786_000;

  private ContentKey() {}

  public static String of(byte[] bytes) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 is required by every Java platform", e);
    }
    byte[] hash = digest.digest(bytes);
    StringBuilder hex = new StringBuilder(hash.length * 2);
    for (byte b : hash) {
      hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
    }
    return hex.toString();
  }

  public static void requireWithinCeiling(byte[] bytes) {
    if (bytes.length > MAX_ARTIFACT_BYTES) {
      throw new ArtifactTooLargeException(
          "artifact of "
              + bytes.length
              + " bytes exceeds the ceiling of "
              + MAX_ARTIFACT_BYTES
              + " bytes");
    }
  }

  public static byte[] pack(byte[] bytes) {
    var out = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(out)) {
      gzip.write(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  public static byte[] unpack(byte[] packed) {
    try (var gzip = new GZIPInputStream(new java.io.ByteArrayInputStream(packed))) {
      return gzip.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
