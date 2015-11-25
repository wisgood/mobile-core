package com.bi.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * 将IP地址转换成长整形
 * 
 * @author fuys
 * 
 */
public class IPFormatUtil {
    private static Logger logger = Logger.getLogger(IPFormatUtil.class
            .getName());

    // private static int ip2Int(String ip) throws UnknownHostException {
    // InetAddress address = InetAddress.getByName(ip);
    // byte[] bytes = address.getAddress();
    // int a, b, c, d;
    // a = byte2int(bytes[0]);
    // b = byte2int(bytes[1]);
    // c = byte2int(bytes[2]);
    // d = byte2int(bytes[3]);
    // int result = (a << 24) | (b << 16) | (c << 8) | d;
    // return result;
    // }
    //
    // private static int byte2int(byte b) {
    // int l = b & 0x07f;
    // if (b < 0) {
    // l |= 0x80;
    // }
    // return l;
    // }
    //
    //
    // public static long ip2long(String ip) throws UnknownHostException {
    // int ipNum = ip2Int(ip);
    // return int2long(ipNum);
    // }
    //
    // private static long int2long(int i) {
    // long l = i & 0x7fffffffL;
    // if (i < 0) {
    // l |= 0x080000000L;
    // }
    // return l;
    // }
    //
    //
    // public static String long2ip(long ip) {
    // int[] b = new int[4];
    // b[0] = (int) ((ip >> 24) & 0xff);
    // b[1] = (int) ((ip >> 16) & 0xff);
    // b[2] = (int) ((ip >> 8) & 0xff);
    // b[3] = (int) (ip & 0xff);
    // String x;
    // Integer p;
    // p = new Integer(0);
    // x = p.toString(b[0]) + "." + p.toString(b[1]) + "." + p.toString(b[2])
    // + "." + p.toString(b[3]);
    //
    // return x;
    //
    // }

    public static String ipFormat(String ipStr) {
        Pattern pattern = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
        Matcher matcher = pattern.matcher(ipStr);
        if (ipStr == null || "".equalsIgnoreCase(ipStr.trim())
                || !(matcher.matches())) {
            ipStr = "0.0.0.0";
        }
        return ipStr;
    }

    public static long ip2long(String ipStr) {

        String retVal = "0";
        long[] nFields = new long[4];
        // Pattern pattern = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
        // Matcher matcher = pattern.matcher(col);
        try {
            // if (col == null || "".equalsIgnoreCase(col.trim())
            // || !(matcher.matches())) {
            // col = "0.0.0.0";
            // }
            ipStr = ipFormat(ipStr);
            String[] strFields = ipStr.split("\\.");
            for (int i = 0; i < 4; i++) {
                nFields[i] = Integer.parseInt(strFields[i]);
            }
            retVal = String.format("%s", (nFields[0] << 24)
                    | (nFields[1] << 16) | (nFields[2] << 8) | nFields[3]);

        }
        catch(Exception e) {
            logger.error("ip格式不对:" + e.getMessage(), e.getCause());
        } finally {
            return Long.parseLong(retVal);
        }
    }
    
    
    public static void main(String[] args)
    {
        System.out.println(ip2long("1.2.9.2"));
        System.out.println(ip2long("1.2.7.2"));
    }
}
