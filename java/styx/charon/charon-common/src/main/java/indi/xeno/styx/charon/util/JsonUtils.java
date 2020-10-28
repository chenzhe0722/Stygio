package indi.xeno.styx.charon.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public abstract class JsonUtils {

  private JsonUtils() {}

  private static final ObjectMapper MAPPER = init();

  private static ObjectMapper init() {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.findAndRegisterModules();
  }

  public static <T> T read(InputStream is, Class<T> cl) throws IOException {
    return MAPPER.readValue(is, cl);
  }

  public static <T> T read(InputStream is, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(is, typeRef);
  }

  public static JsonNode read(InputStream is) throws IOException {
    return MAPPER.readTree(is);
  }

  public static <T> T read(Reader reader, Class<T> cl) throws IOException {
    return MAPPER.readValue(reader, cl);
  }

  public static <T> T read(Reader reader, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(reader, typeRef);
  }

  public static JsonNode read(Reader reader) throws IOException {
    return MAPPER.readTree(reader);
  }

  public static <T> T read(File file, Class<T> cl) throws IOException {
    return MAPPER.readValue(file, cl);
  }

  public static <T> T read(File file, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(file, typeRef);
  }

  public static JsonNode read(File file) throws IOException {
    return MAPPER.readTree(file);
  }

  public static <T> T read(String str, Class<T> cl) throws JsonProcessingException {
    return MAPPER.readValue(str, cl);
  }

  public static <T> T read(String str, TypeReference<T> typeRef) throws JsonProcessingException {
    return MAPPER.readValue(str, typeRef);
  }

  public static JsonNode read(String str) throws JsonProcessingException {
    return MAPPER.readTree(str);
  }

  public static <T> T read(JsonNode node, Class<T> cl) throws IOException {
    return MAPPER.readValue(node.traverse(), cl);
  }

  public static <T> T read(JsonNode node, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(node.traverse(), typeRef);
  }
}
