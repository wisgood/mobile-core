package com.bi.common.util;

public class PlatTypeFormatUtil {

    public static String getFormatPlatType(String origPlatTypeStr) {
        origPlatTypeStr = origPlatTypeStr.toLowerCase();
        if (origPlatTypeStr.indexOf("iphone") >= 0) {
            return "iphone";
        }
        else if (origPlatTypeStr.indexOf("aphone") >= 0) {
            return "aphone";
        }
        else if (origPlatTypeStr.indexOf("ipad") >= 0) {
            return "ipad";
        }
        else if (origPlatTypeStr.indexOf("apad") >= 0) {
            return "apad";
        }
        else if (origPlatTypeStr.indexOf("winphone") >= 0) {
            return "winphone";
        }
        else if (origPlatTypeStr.indexOf("winpad") >= 0) {
            return "winpad";
        } else if (origPlatTypeStr.indexOf("flash") >= 0) {
            return "flash";
        }
        return "other";
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
