package com.bi.common.dm.pojo;

import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;

/**
 * #infohash, 分集ID，媒体ID，频道ID
 * 
 * @author fuys
 * 
 */
public enum DMInfoHashEnum {
    IH(DefaultFieldValueEnum.infohashidDefault.getValueStr()), SERIAL_ID(
            DefaultFieldValueEnum.serialIdDefault.getValueStr()), MEDIA_ID(
            DefaultFieldValueEnum.mediaIdDefault.getValueStr()), CHANNEL_ID(
            DefaultFieldValueEnum.channelIdDefault.getValueStr()), MEDIA_NAME(
            DefaultFieldValueEnum.mediaNameDefault.getValueStr());

    private DMInfoHashEnum(String valueStr) {
        this.valueStr = valueStr;
    }

    private String valueStr;

    public String getDefaultStr() {
        return valueStr;
    }

    public String getInForHashDefaultValue() {
        String resultValue = new String();
        DMInfoHashEnum[] dmInfoHashEnums = this.values();
        for (int i = 0; i < dmInfoHashEnums.length; i++) {
            resultValue += dmInfoHashEnums[i].getDefaultStr();
            if (i < dmInfoHashEnums.length - 1) {
                resultValue += UtilComstrantsEnum.tabSeparator.getValueStr();
            }

        }
        return resultValue;
    }
}
