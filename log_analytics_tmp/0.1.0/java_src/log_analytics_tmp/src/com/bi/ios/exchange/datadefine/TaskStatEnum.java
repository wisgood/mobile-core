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
public enum TaskStatEnum
{
	SESSION_ID,
	CLIENTIP_DEC,
	TIME_STAMP,
	PACKAGE_ID,
	PVS,
	BTD,
	BTP,
	BTSD,
	BTST,
	DOWNLOAD_RATE,
	FTD,
	FTP,
	FTSD,
	FTST,
	MAC_HEX,
	MDT,
	UPLOAD_RATE,
	VERSION_DOT,
	PROVINCE_ID,
	AREA_ID,
	ISP_ID;

	public static boolean containsField(String fieldName)
	{
		TaskStatEnum[] enumFields = TaskStatEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (TaskStatEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		TaskStatEnum[] enumFields = TaskStatEnum.values();
		for (TaskStatEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(TaskStatEnum.class.getName(), fieldName);
	}

}
