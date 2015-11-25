package com.bi.ios.exchange.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ios.exchange.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

public enum BootStrapEnum
{
	DATE_ID,
	HOUR_ID,
	PLAT_ID,
	VERSION_ID,
	QUDAO_ID,
	BOOT_TYPE,
	MACCLEAN,
	PROVINCE_ID,
	CITY_ID,
	ISP_ID,
	OK_TYPE,
	VERSION_STR,
	TIMESTAMP,
	IP,
	DEV,
	MAC,
	VER,
	NT,
	BTYPE,
	BTIME,
	OK,
	SR,
	MEM,
	TDISK,
	FDISK,
	SID,
	RT,
	IPHONEIP,
	BROKEN,
	IMEI;

	public static boolean containsField(String fieldName)
	{
		BootStrapEnum[] enumFields = BootStrapEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (BootStrapEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		BootStrapEnum[] enumFields = BootStrapEnum.values();
		for (BootStrapEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(BootStrapEnum.class.getName(), fieldName);
	}

}
