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
public enum PlayErrorEnum
{
	O_TIMESTAMP,
	O_IP,
	O_OSCODE,
	O_MAC,
	O_CHANNELID,
	O_VERSION,
	O_ERRORCODE,
	O_MOVIENAME,
	O_PLAYTYPE,
	O_CLOSECODE,
	O_QQCLICK,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID,
	T_VERSION,
	T_MOVIEFORM;

	public static boolean containsField(String fieldName)
	{
		PlayErrorEnum[] enumFields = PlayErrorEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (PlayErrorEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		PlayErrorEnum[] enumFields = PlayErrorEnum.values();
		for (PlayErrorEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(PlayErrorEnum.class.getName(), fieldName);
	}

}
