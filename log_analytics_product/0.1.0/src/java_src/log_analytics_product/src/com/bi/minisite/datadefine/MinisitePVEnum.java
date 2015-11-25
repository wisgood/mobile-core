package com.bi.minisite.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.minisite.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 8
 *        transform fields number is 7
 * 
 */
public enum MinisitePVEnum
{
	O_TIMESTAMP,
	O_IP,
	O_TAB,
	O_MAC,
	O_MODE,
	O_SHOWNUM,
	O_VERSION,
	O_FCK,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID,
	T_VERSION;

	public static boolean containsField(String fieldName)
	{
		MinisitePVEnum[] enumFields = MinisitePVEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (MinisitePVEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		MinisitePVEnum[] enumFields = MinisitePVEnum.values();
		for (MinisitePVEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(MinisitePVEnum.class.getName(), fieldName);
	}
}
