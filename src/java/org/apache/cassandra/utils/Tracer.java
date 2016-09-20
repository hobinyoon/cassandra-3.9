package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public class Tracer {
    public static String GetCallStack() {
        StringBuilder sb = new StringBuilder(1024);
        boolean first = true;
        int i = 0;
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            i ++;
            if (i <= 2) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                sb.append("\n");
            }
            sb.append(ste);
        }
        return Indent(sb.toString(), 2);
    }

    public static String Indent(String in, int ind) {
        StringBuilder indStr = new StringBuilder(10);
        for (int i = 0; i < ind; i ++)
            indStr.append(" ");
        return in.replaceAll("(?m)^", indStr.toString());
    }

    public static String toHex(ByteBuffer bb) {
        return toHex(bb.array());
    }

    // http://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static long toLong(ByteBuffer bb) {
        return toLong(bb.array());
    }

    public static long toLong(byte[] bytes) {
        long i = 0;
        for (int j = 0; j < bytes.length; j++) {
            i = (i << 8) + bytes[j];
        }
        return i;
    }
}
