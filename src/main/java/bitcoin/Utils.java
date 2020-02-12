package bitcoin;

import javax.xml.crypto.dsig.DigestMethod;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;

public class Utils {

    public static long readVarInt(ByteBuffer in) {
        return getVarInt(getVIBytes(in));
    }

    public static long getVarInt(byte[] bytes) {
        long result=0;
        if (bytes.length==0) return result;

        int ub=bytes[0] & 0xFF;
        if (ub<0xFD) return ub;
        int size=0;
        switch(ub) {
            case 0xfd: { size=3; break; }
            case 0xfe: { size=5; break; }
            case 0xff: { size=9; break; }
        }
        byte[] data=reverseBytes(Arrays.copyOfRange(bytes, 1, size));
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        switch(size) {
            case 3: { result=byteBuffer.getShort(); break; }
            case 5: { result=byteBuffer.getInt(); break; }
            case 9: { result=byteBuffer.getLong(); break; }
        }
        return result;
    }

    private static byte getVISize(byte b) {
        switch(b&0xff) {
            case 0xfd: return 3;
            case 0xfe: return 5;
            case 0xff: return 9;
        }
        return 1;
    }

    private static byte[] getVIBytes(ByteBuffer in) {
        byte size=in.get();
        byte visize=getVISize(size);
        byte[] bytes=new byte[visize];
        bytes[0]=size;
        in.get(bytes,1,visize-1);
        return bytes;
    }


    public static byte[] reverseBytes(byte[] bytes) {
        byte[] buf = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++)
            buf[i] = bytes[bytes.length - 1 - i];
        return buf;
    }

    private static final char[] upper  = "0123456789ABCDEF".toCharArray();
    private static final char[] lower  = "0123456789abcdef".toCharArray();

    public static String encodeBase16(byte[] src, boolean isUpper) {
        char[] table = isUpper ? upper : lower;
        char[] dst   = new char[src.length * 2];
        for (int si = 0, di = 0; si < src.length; si++) {
            byte b = src[si];
            dst[di++] = table[(b & 0xf0) >>> 4];
            dst[di++] = table[(b & 0x0f)];
        }
        return new String(dst);
    }

    public static final char[] ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();
    private static final char ENCODED_ZERO = ALPHABET[0];


    public static String encode58(byte[] input) {
        if (input.length == 0) {
            return "";
        }
        int zeros = 0;
        while (zeros < input.length && input[zeros] == 0) {
            ++zeros;
        }
        input = Arrays.copyOf(input, input.length);
        char[] encoded = new char[input.length * 2];
        int outputStart = encoded.length;
        for (int inputStart = zeros; inputStart < input.length; ) {
            encoded[--outputStart] = ALPHABET[divmod(input, inputStart, 256, 58)];
            if (input[inputStart] == 0) {
                ++inputStart;
            }
        }
        while (outputStart < encoded.length && encoded[outputStart] == ENCODED_ZERO) {
            ++outputStart;
        }
        while (--zeros >= 0) {
            encoded[--outputStart] = ENCODED_ZERO;
        }
        return new String(encoded, outputStart, encoded.length - outputStart);
    }

    public static byte[] hashTwice(byte[] input) {
        MessageDigest digest = null;
        try{ digest =  MessageDigest.getInstance("SHA-256");
        }catch (NoSuchAlgorithmException e) { return null; }
        digest.update(input, 0, input.length);
        return digest.digest(digest.digest());
    }

    public static byte[] hash160(byte[] input) {

        MessageDigest digest = null;
        try{ digest =  MessageDigest.getInstance("SHA-256");
        }catch (NoSuchAlgorithmException e) { return null; }
        digest.update(input, 0, input.length);
        byte[] sha256= digest.digest();

        RIPEMD160Digest digest2 = new RIPEMD160Digest();
        digest2.update(sha256, 0, sha256.length);
        byte[] bytes = new byte[20];
        digest2.doFinal(bytes, 0);
        return bytes;
    }

    public static final String toBase58(byte[] bytes,int version) {
        byte[] addressBytes = new byte[1 + bytes.length + 4];
        addressBytes[0] = (byte) version;
        System.arraycopy(bytes, 0, addressBytes, 1, bytes.length);
        MessageDigest digest = null;
        try{ digest =  MessageDigest.getInstance("SHA-256");
        }catch (NoSuchAlgorithmException e) { return null; }
        digest.update(addressBytes, 0,  bytes.length + 1);
        byte[] checksum  = digest.digest(digest.digest());
        System.arraycopy(checksum, 0, addressBytes, bytes.length + 1, 4);
        return Utils.encode58(addressBytes);
    }

    private static byte divmod(byte[] number, int firstDigit, int base, int divisor) {
        int remainder = 0;
        for (int i = firstDigit; i < number.length; i++) {
            int digit = (int) number[i] & 0xFF;
            int temp = remainder * base + digit;
            number[i] = (byte) (temp / divisor);
            remainder = temp % divisor;
        }
        return (byte) remainder;
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(byte b:bytes) sb.append(Integer.toHexString(b | 0xffff00));
        return sb.toString();
    }
}
