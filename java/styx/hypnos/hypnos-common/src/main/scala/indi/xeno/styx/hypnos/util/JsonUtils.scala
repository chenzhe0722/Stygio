package indi.xeno.styx.hypnos.util

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper.builder

import java.io.{File, InputStream, Reader}

object JsonUtils {

  private val OBJECT_MAPPER = builder().build().findAndRegisterModules()

  def readJson[T](is: InputStream, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(is, cl)
  }

  def readJson[T](is: InputStream, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(is, typeRef)
  }

  def readJson(is: InputStream): JsonNode = {
    OBJECT_MAPPER.readTree(is)
  }

  def readJson[T](reader: Reader, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(reader, cl)
  }

  def readJson[T](reader: Reader, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(reader, typeRef)
  }

  def readJson(reader: Reader): JsonNode = {
    OBJECT_MAPPER.readTree(reader)
  }

  def readJson[T](file: File, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(file, cl)
  }

  def readJson[T](file: File, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(file, typeRef)
  }

  def readJson(file: File): JsonNode = {
    OBJECT_MAPPER.readTree(file)
  }

  def readJson[T](str: String, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(str, cl)
  }

  def readJson[T](str: String, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(str, typeRef)
  }

  def readJson(str: String): JsonNode = {
    OBJECT_MAPPER.readTree(str)
  }

  def readJson[T](node: JsonNode, cl: Class[T]): T = {
    OBJECT_MAPPER.readValue(node.traverse(), cl)
  }

  def readJson[T](node: JsonNode, typeRef: TypeReference[T]): T = {
    OBJECT_MAPPER.readValue(node.traverse(), typeRef)
  }
}
