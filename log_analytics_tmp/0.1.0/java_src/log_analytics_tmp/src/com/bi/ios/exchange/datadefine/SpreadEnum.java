package com.bi.ios.exchange.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ios.exchange.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 8
 *        transform fields number is 6
 * 
 */
public enum SpreadEnum
{
	O_TIMESTAMP,
	O_IP,
	O_DEVICE,
	O_STREAMID,
	O_ENVIP,
	O_APPURL1,
	O_APPURL2,
	O_APPURL3,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		SpreadEnum[] enumFields = SpreadEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (SpreadEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		SpreadEnum[] enumFields = SpreadEnum.values();
		for (SpreadEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(SpreadEnum.class.getName(), fieldName);
	}

}
