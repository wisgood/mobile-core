package com.bi.ibidian.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @author wangzg
 * 
 * @DESC: original fields number is 7
 *        transform fields number is 6
 * 
 */
public enum HotsSpeedEnum
{
	O_TIMESTAMP,
	O_IP,
	O_FCK,
	O_MAC,
	O_FPC,
	O_URL,
	O_VTIME,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		HotsSpeedEnum[] fields = HotsSpeedEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (HotsSpeedEnum hotsSpeedEnum : fields)
		{
			fieldsSet.add(hotsSpeedEnum.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		HotsSpeedEnum[] fields = HotsSpeedEnum.values();
		for (HotsSpeedEnum singleEnum : fields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(HotsSpeedEnum.class.getName(), fieldName);
	}

}
