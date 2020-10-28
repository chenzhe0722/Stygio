package indi.xeno.styx.erebos.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import static com.fasterxml.jackson.dataformat.yaml.YAMLMapper.builder;

public abstract class YamlUtils {

  private YamlUtils() {}

  private static final ObjectMapper MAPPER = builder().build().findAndRegisterModules();

  public static <T> T readYaml(InputStream is, Class<T> cl) throws IOException {
    return MAPPER.readValue(is, cl);
  }

  public static <T> T readYaml(InputStream is, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(is, typeRef);
  }

  public static JsonNode readYaml(InputStream is) throws IOException {
    return MAPPER.readTree(is);
  }

  public static <T> T readYaml(Reader reader, Class<T> cl) throws IOException {
    return MAPPER.readValue(reader, cl);
  }

  public static <T> T readYaml(Reader reader, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(reader, typeRef);
  }

  public static JsonNode readYaml(Reader reader) throws IOException {
    return MAPPER.readTree(reader);
  }

  public static <T> T readYaml(File file, Class<T> cl) throws IOException {
    return MAPPER.readValue(file, cl);
  }

  public static <T> T readYaml(File file, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(file, typeRef);
  }

  public static JsonNode readYaml(File file) throws IOException {
    return MAPPER.readTree(file);
  }

  public static <T> T readYaml(String str, Class<T> cl) throws JsonProcessingException {
    return MAPPER.readValue(str, cl);
  }

  public static <T> T readYaml(String str, TypeReference<T> typeRef)
      throws JsonProcessingException {
    return MAPPER.readValue(str, typeRef);
  }

  public static JsonNode readYaml(String str) throws JsonProcessingException {
    return MAPPER.readTree(str);
  }

  public static <T> T readYaml(JsonNode node, Class<T> cl) throws IOException {
    return MAPPER.readValue(node.traverse(), cl);
  }

  public static <T> T readYaml(JsonNode node, TypeReference<T> typeRef) throws IOException {
    return MAPPER.readValue(node.traverse(), typeRef);
  }
}
