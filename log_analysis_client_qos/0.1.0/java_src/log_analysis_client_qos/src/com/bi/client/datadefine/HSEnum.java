package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 17
 *        transform fields number is 6
 * 
 */
public enum HSEnum
{
	O_SESSIONID,
	O_CLIENTIP,
	O_TIMESTAMP,
	O_PACKAGEID,
	O_PVS,
	O_HOSTIP,
	O_LOGINMODE,
	O_MAC,
	O_MESSAGETYPE,
	O_NETTYPE,
	O_HOSTPORT,
	O_RHOSTIP,
	O_TIMEUSED,
	O_VERSION,
	O_PROVINCEID,
	O_AREAID,
	O_ISPID,
	T_DATEID,
	T_HOURID,
	T_CLIENTIP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		HSEnum[] enumFields = HSEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (HSEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		HSEnum[] enumFields = HSEnum.values();
		for (HSEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(HSEnum.class.getName(), fieldName);
	}

}
