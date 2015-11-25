package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 11
 *        transform fields number is 8
 * 
 */
public enum BootEnum
{
	DATE_ID,
	VERSION_DEC,
	AREA_ID,
	MAC_HEX,
	ISP_ID,
	STARTTYPE_ID,
	TIMESTAMP,
	IP_DEC,
	CHANNEL_ID,
	OS_DOT,
	ERROR_CODE,
	HELLO_COUNT,
	ACCELERATE_CODE,
	LANGUAGE_CODE,
	TRAYLIMIT_CODE;

	public static boolean containsField(String fieldName)
	{
		BootEnum[] enumFields = BootEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (BootEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		BootEnum[] enumFields = BootEnum.values();
		for (BootEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(BootEnum.class.getName(), fieldName);
	}

}
