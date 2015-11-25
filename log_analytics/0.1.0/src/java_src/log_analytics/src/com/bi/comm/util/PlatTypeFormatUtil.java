package com.bi.comm.util;

public class PlatTypeFormatUtil {
    // origPlatTypeStr
    public static String getFormatPlatType(String origPlatTypeStr) {
        origPlatTypeStr = origPlatTypeStr.toLowerCase();
        String devInfo = origPlatTypeStr;
        int endIndex = origPlatTypeStr.indexOf('_');
        if (endIndex > 0) {
            devInfo = new String(origPlatTypeStr.substring(0, endIndex));
        }

        if (devInfo.trim().equalsIgnoreCase("ott")) {
            if (origPlatTypeStr.contains("android")) {
                return "android";
            }
            return "other";
        }
        return devInfo;

    }

    public static void filterFlash(String origPlatTypeStr) throws Exception {
        if (origPlatTypeStr.toLowerCase().contains("flash")) {
            throw new Exception("contain flash value");
        }
    }

    public static void filterOtherInfo(String origPlatTypeStr, String errorInfo)
            throws Exception {
        if (origPlatTypeStr.toLowerCase().contains(errorInfo)) {
            throw new Exception("contain " + errorInfo + " value");
        }
    }
}
