package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 8
 *        transform fields number is 7
 * 
 */
public enum DumpEnum
{
	O_TIMESTAMP,
	O_IP,
	O_VERSION,
	O_MODULE,
	O_MAC,
	O_CRASHDATE,
	O_CRASHTIME,
	O_FILENAME,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID,
	T_VERSION;

	public static boolean containsField(String fieldName)
	{
		DumpEnum[] enumFields = DumpEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (DumpEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		DumpEnum[] enumFields = DumpEnum.values();
		for (DumpEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(DumpEnum.class.getName(), fieldName);
	}

}
