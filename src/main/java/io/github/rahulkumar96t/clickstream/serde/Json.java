package io.github.rahulkumar96t.clickstream.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** The shared Jackson mapper and JSON {@link Serde}s for the model records. */
public final class Json {

  public static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .defaultPropertyInclusion(
              JsonInclude.Value.construct(
                  JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL))
          .build();

  private Json() {}

  /** A serde that writes {@code type} as JSON. {@code null} maps to {@code null} both ways. */
  public static <T> Serde<T> serde(Class<T> type) {
    return Serdes.serdeFrom(
        (topic, value) -> {
          try {
            return value == null ? null : MAPPER.writeValueAsBytes(value);
          } catch (IOException e) {
            throw new SerializationException("cannot serialize " + type.getSimpleName(), e);
          }
        },
        (topic, bytes) -> {
          try {
            return bytes == null ? null : MAPPER.readValue(bytes, type);
          } catch (IOException e) {
            throw new SerializationException("cannot deserialize " + type.getSimpleName(), e);
          }
        });
  }

  public static byte[] bytes(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
