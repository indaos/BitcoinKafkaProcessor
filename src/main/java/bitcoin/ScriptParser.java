package bitcoin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ScriptParser {

  public static final int OP_PUSHDATA1 = 0x4c;
  public static final int OP_PUSHDATA2 = 0x4d;
  public static final int OP_PUSHDATA4 = 0x4e;
  public static final int OP_DUP = 0x76;
  public static final int OP_EQUALVERIFY = 0x88;
  public static final int OP_HASH160 = 0xa9;
  public static final int OP_CHECKSIG = 0xac;
  public static final int OP_EQUAL = 0x87;
  public static final int OP_CHECKMULTISIG = 0xae;

  static class Element {

    int opcode;
    byte[] bytes;

    Element(int opcode, byte[] bytes) {
      this.opcode = opcode;
      this.bytes = bytes;
    }
  }

  public static boolean isToAddress(List<Element> elements) {
    if (elements.size() == 5) {
      return elements.get(0).opcode == OP_DUP && elements.get(1).opcode == OP_HASH160 &&
          elements.get(2).bytes.length == 20 && elements.get(3).opcode == OP_EQUALVERIFY &&
          elements.get(4).opcode == OP_CHECKSIG;
    }
    return false;
  }

  public static boolean isToScriptHash(List<Element> elements) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    IntStream.range(0, elements.size())
        .forEach(
            e -> {
              try {
                  if (elements.get(e).bytes != null) {
                      os.write(elements.get(e).bytes);
                  }
              } catch (IOException ignored) {
              }
            });
    byte[] script = os.toByteArray();
    return script.length == 23 &&
        (script[0] & 0xff) == OP_HASH160 &&
        (script[1] & 0xff) == 0x14 &&
        (script[22] & 0xff) == OP_EQUAL;
  }

  public static byte[] getPubKey(List<Element> elements) {
      if (elements.size() != 2) {
          return null;
      }

    final Element e0 = elements.get(0);
    final byte[] e0Bytes = e0.bytes;
    final Element e1 = elements.get(1);
    final byte[] e1Bytes = e1.bytes;
    if (e0Bytes != null && e0Bytes.length > 2
        && e1Bytes != null && e1Bytes.length > 2) {
      return e1Bytes;
    } else if (e1.opcode == OP_CHECKSIG
        && e0Bytes != null && e0Bytes.length > 2) {
      return e0Bytes;
    }
    return null;
  }


  public static String getToAddress(byte[] bytes) {

    List<Element> elements = parse(bytes);
      if (elements == null || elements.size() == 0) {
          return "";
      }
      if (isToAddress(elements)) {
          return Utils.toBase58(elements.get(2).bytes, 0);
      } else if (isToScriptHash(elements)) {
          return Utils.toBase58(elements.get(1).bytes, 5);
      } else {
          byte[] toparse = bytes;
          if (elements.size() == 1 && elements.get(0).bytes != null) {
              toparse = elements.get(0).bytes;
          }
          List<Element> cls = parse(toparse);
          if (cls == null) {
              return null;
          }
          byte[] key = getPubKey(cls);
          if (key == null) {
              return null;
          }
          byte[] res = Utils.hash160(key);
        assert res != null;
        return Utils.toBase58(res, 0);
      }
  }

  private static List<Element> parse(byte[] body) {

    List<Element> elements = new ArrayList<>();
    ByteArrayInputStream bis = new ByteArrayInputStream(body);
    while (bis.available() > 0) {
      int opc = bis.read();
      long nread = -1;
      Element element;

      if (opc >= 0 && opc < OP_PUSHDATA1) {
        nread = opc;
      } else if (opc == OP_PUSHDATA1) {
          if (bis.available() < 1) {
              return null;
          }
        nread = bis.read();
      } else if (opc == OP_PUSHDATA2) {
          if (bis.available() < 2) {
              return null;
          }
        nread = bis.read() | (bis.read() << 8);
      } else if (opc == OP_PUSHDATA4) {
          if (bis.available() < 4) {
              return null;
          }
        nread = ((long) bis.read())
            | (((long) bis.read()) << 8)
            | (((long) bis.read()) << 16)
            | (((long) bis.read()) << 24);
      }
      if (nread == -1) {
        element = new Element(opc, null);
      } else {
          if (nread > bis.available() || nread == 0) {
              return null;
          }
        byte[] data = new byte[(int) nread];
        int res = bis.read(data, 0, (int) nread);
          if (res != nread) {
              return null;
          }
        element = new Element(opc, data);
      }

      elements.add(element);
    }
    return elements;
  }
}
