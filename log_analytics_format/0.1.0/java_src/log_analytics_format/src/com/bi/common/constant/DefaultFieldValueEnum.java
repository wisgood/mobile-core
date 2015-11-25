package com.bi.common.constant;

public enum DefaultFieldValueEnum {
    hourIdDefault(0), provinceIdefault(-999), cityIdDefault(-999), ispIdDefault(
            -999), platIdDefault(-999), qudaoIdDefault(-999), versionIdDefault(
            -999), serverIdDefault(-999), clientIpDefault("0.0.0.0"), macCodeDefault(
            "000000000000"), fudidDefault(
            "0000000000000000000000000000000000000000000000000000000000000000"), infohashidDefault(
            "0000000000000000000000000000000000000000"), serialIdDefault(-999), mediaIdDefault(
            -999), channelIdDefault(-999), mediaNameDefault("Unknown"), mediaTypeIdDefault(
            -999), netTypeDefault(-999), OKDefault(-999), bufferPosDefault(-999), bufferTimeDefault(
            -999), drateDefault(-999), nrateDefault(-999), MSOKDefault(-999), playerTypeDefault(
            -999), CLDefault(-999), messageIdDefault(-999), lianDefault(-999), fckDefault(
            "000000000000000000000000000000000000000000000000"), uidDefault("0"), IPDefault(
            "0.0.0.0"),numDefault(-999),numIndexDefault(0),srDefault("0*0"),okBootIsSuccess(-1),pbreDefault(10);
    private DefaultFieldValueEnum(String valueStr) {
        this.valueStr = valueStr;
    }

    private DefaultFieldValueEnum(int valueInt) {
        this.valueInt = valueInt;
    }

    private int valueInt;

    private String valueStr;

    public String getValueStr() {

        return null == valueStr ? String.valueOf(valueInt) : valueStr;
    }

    public int getValueInt() {

        return valueInt;
    }
}
