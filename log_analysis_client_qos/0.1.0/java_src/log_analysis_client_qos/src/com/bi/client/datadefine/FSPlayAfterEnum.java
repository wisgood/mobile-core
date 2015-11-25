package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

public enum FSPlayAfterEnum
{
	DATE_ID,
	HOUR_ID,
	PLAT_ID,
	CHANNEL_ID,
	CITY_ID,
	MAC_CODE,
	MEDIA_ID,
	SERIAL_ID,
	PROVINCE_ID,
	ISP_ID,
	TIMESTAMP,
	IP,
	MID,
	UID,
	MAC,
	FCK,
	IH;

	public static boolean containsField(String fieldName)
	{
		FSPlayAfterEnum[] enumFields = FSPlayAfterEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (FSPlayAfterEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		FSPlayAfterEnum[] enumFields = FSPlayAfterEnum.values();
		for (FSPlayAfterEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(FSPlayAfterEnum.class.getName(), fieldName);
	}

}
