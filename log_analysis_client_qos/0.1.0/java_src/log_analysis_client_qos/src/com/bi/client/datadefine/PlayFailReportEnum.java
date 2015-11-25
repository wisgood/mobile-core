package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 6
 *        transform fields number is 7
 * 
 */
public enum PlayFailReportEnum
{
	O_TIMESTAMP,
	O_IP,
	O_MAC,
	O_VERSION,
	O_INFOHASH,
	O_FAILCODE,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID,
	T_VERSION;

	public static boolean containsField(String fieldName)
	{
		PlayFailReportEnum[] enumFields = PlayFailReportEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (PlayFailReportEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		PlayFailReportEnum[] enumFields = PlayFailReportEnum.values();
		for (PlayFailReportEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(PlayFailReportEnum.class.getName(), fieldName);
	}

}
