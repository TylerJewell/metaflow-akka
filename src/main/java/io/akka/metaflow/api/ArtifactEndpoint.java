package io.akka.metaflow.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import io.akka.metaflow.application.BlobEntity;
import io.akka.metaflow.domain.ArtifactTooLargeException;
import io.akka.metaflow.domain.ContentKey;

/**
 * Storing and loading artifact bytes (SPEC-001 §3 rules 1-5).
 *
 * <p>The caller supplies bytes and gets back the key they are stored under, so a caller that has
 * already serialized something can address it without this service knowing what it is.
 */
@HttpEndpoint("/artifacts")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class ArtifactEndpoint {

  public record StoredArtifact(String key, boolean stored, int size) {}

  private final ComponentClient componentClient;

  public ArtifactEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  @Post
  public StoredArtifact store(akka.http.javadsl.model.HttpEntity.Strict body) {
    byte[] bytes = body.getData().toArray();
    try {
      ContentKey.requireWithinCeiling(bytes);
    } catch (ArtifactTooLargeException e) {
      throw HttpException.error(
          akka.http.javadsl.model.StatusCodes.PAYLOAD_TOO_LARGE, e.getMessage());
    }
    String key = ContentKey.of(bytes);
    var result =
        componentClient.forKeyValueEntity(key).method(BlobEntity::store).invoke(bytes);
    return new StoredArtifact(result.key(), result.stored(), result.size());
  }

  @Get("/{key}")
  public akka.http.javadsl.model.HttpResponse load(String key) {
    byte[] bytes;
    try {
      bytes = componentClient.forKeyValueEntity(key).method(BlobEntity::load).invoke().bytes();
    } catch (akka.javasdk.CommandException e) {
      // Only the entity's own refusal means "not stored"; anything else is a real failure and
      // is left to surface as one rather than being reported as an absent artifact.
      throw HttpException.notFound();
    }
    return akka.javasdk.http.HttpResponses.of(
        akka.http.javadsl.model.StatusCodes.OK,
        akka.http.javadsl.model.ContentTypes.APPLICATION_OCTET_STREAM,
        bytes);
  }
}
