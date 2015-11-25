package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @DESC: original fields number is 15
 *        transform fields number is 6
 * 
 */
public enum LoginEnum
{
	O_SESSIONID,
	O_CLIENTIP,
	O_TIMESTAMP,
	O_PACKAGEID,
	O_PVS,
	O_LOGINMODE,
	O_LOGINNUM,
	O_MAC,
	O_REASON,
	O_SERVERIP,
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
		LoginEnum[] enumFields = LoginEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (LoginEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		LoginEnum[] enumFields = LoginEnum.values();
		for (LoginEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(LoginEnum.class.getName(), fieldName);
	}

}
