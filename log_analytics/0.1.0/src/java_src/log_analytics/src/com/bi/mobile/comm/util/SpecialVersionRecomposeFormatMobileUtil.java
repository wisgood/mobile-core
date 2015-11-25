package com.bi.mobile.comm.util;

public class SpecialVersionRecomposeFormatMobileUtil {

    public static String[] recomposeBySpecialVersion(String[] splitSts,
            String enumClassStr) throws ClassNotFoundException {

        String[] splitStsValue = splitSts;
        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String versionInfo = splitStsValue[Enum.valueOf(logEnum, "SID")
                .ordinal() - 1];
        if ("1.2.0.2".equalsIgnoreCase(versionInfo)
                || "1.2.0.1".equalsIgnoreCase(versionInfo)) {

            splitStsValue = new String[splitSts.length];

            for (int i = 0; i < Enum.valueOf(logEnum, "VER").ordinal(); i++) {
                splitStsValue[i] = splitSts[i];
            }
            splitStsValue[Enum.valueOf(logEnum, "VER").ordinal()] = versionInfo;
            for (int i = Enum.valueOf(logEnum, "VER").ordinal(); i < splitSts.length; i++) {
                if (i < splitSts.length - 1) {
                    splitStsValue[i + 1] = splitSts[i];
                }
                else {
                    splitStsValue[i] = splitSts[i];
                }
            }
        }
        return splitStsValue;
    }

    public static String[] recomposeBySpecialVersion(String[] splitSts,
            String enumClassStr, String keyColmStr)
            throws ClassNotFoundException {

        String[] splitStsValue = splitSts;
        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String versionInfo = splitStsValue[Enum.valueOf(logEnum, keyColmStr)
                .ordinal() - 1];
        if ("1.2.0.2".equalsIgnoreCase(versionInfo)
                || "1.2.0.1".equalsIgnoreCase(versionInfo)) {

            splitStsValue = new String[splitSts.length];

            for (int i = 0; i < Enum.valueOf(logEnum, "VER").ordinal(); i++) {
                splitStsValue[i] = splitSts[i];
            }
            splitStsValue[Enum.valueOf(logEnum, "VER").ordinal()] = versionInfo;
            for (int i = Enum.valueOf(logEnum, "VER").ordinal(); i < splitSts.length; i++) {
                if (i < splitSts.length - 1) {
                    splitStsValue[i + 1] = splitSts[i];
                }
                else {
                    splitStsValue[i] = splitSts[i];
                }
            }
        }
        return splitStsValue;
    }
    
    public static String[] recomposeBySpecialVersionIndex(String[] splitSts,
            String enumClassStr)
            throws ClassNotFoundException {

        String[] splitStsValue = splitSts;
        Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
        String versionInfo = splitStsValue[splitStsValue.length - 2];
        if ("1.2.0.2".equalsIgnoreCase(versionInfo)
                || "1.2.0.1".equalsIgnoreCase(versionInfo)) {

            splitStsValue = new String[splitSts.length];

            for (int i = 0; i < Enum.valueOf(logEnum, "VER").ordinal(); i++) {
                splitStsValue[i] = splitSts[i];
            }
            splitStsValue[Enum.valueOf(logEnum, "VER").ordinal()] = versionInfo;
            for (int i = Enum.valueOf(logEnum, "VER").ordinal(); i < splitSts.length; i++) {
                if (i < splitSts.length - 1) {
                    splitStsValue[i + 1] = splitSts[i];
                }
                else {
                    splitStsValue[i] = splitSts[i];
                }
            }
        }
        return splitStsValue;
    }

}
