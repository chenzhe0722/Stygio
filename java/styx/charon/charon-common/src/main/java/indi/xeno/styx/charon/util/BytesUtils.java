package indi.xeno.styx.charon.util;

import java.nio.ByteOrder;

import static java.nio.ByteBuffer.allocate;

public abstract class BytesUtils {

  private BytesUtils() {}

  public static byte[] byteToBytes(byte bt) {
    return allocate(1).put(bt).array();
  }

  public static byte[] shortToBytes(short num, ByteOrder ord) {
    return allocate(2).order(ord).putShort(num).array();
  }

  public static byte[] intToBytes(int num, ByteOrder ord) {
    return allocate(4).order(ord).putInt(num).array();
  }

  public static byte[] longToBytes(long num, ByteOrder ord) {
    return allocate(8).order(ord).putLong(num).array();
  }

  public static byte[] floatToBytes(float num, ByteOrder ord) {
    return allocate(4).order(ord).putFloat(num).array();
  }

  public static byte[] doubleToBytes(double num, ByteOrder ord) {
    return allocate(8).order(ord).putDouble(num).array();
  }
}
