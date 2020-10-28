package indi.xeno.styx.hypnos.util

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLMapper.builder

import java.io.{File, InputStream, Reader}

object YamlUtils {

  private val OBJECT_MAPPER = builder().build().findAndRegisterModules()

  def readYaml[T](is: InputStream, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(is, cl)
  }

  def readYaml[T](is: InputStream, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(is, typeRef)
  }

  def readYaml(is: InputStream): JsonNode = {
    OBJECT_MAPPER.readTree(is)
  }

  def readYaml[T](reader: Reader, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(reader, cl)
  }

  def readYaml[T](reader: Reader, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(reader, typeRef)
  }

  def readYaml(reader: Reader): JsonNode = {
    OBJECT_MAPPER.readTree(reader)
  }

  def readYaml[T](file: File, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(file, cl)
  }

  def readYaml[T](file: File, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(file, typeRef)
  }

  def readYaml(file: File): JsonNode = {
    OBJECT_MAPPER.readTree(file)
  }

  def readYaml[T](str: String, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(str, cl)
  }

  def readYaml[T](str: String, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(str, typeRef)
  }

  def readYaml(str: String): JsonNode = {
    OBJECT_MAPPER.readTree(str)
  }

  def readYaml[T](node: JsonNode, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(node.traverse(), cl)
  }

  def readYaml[T](node: JsonNode, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(node.traverse(), typeRef)
  }
}
