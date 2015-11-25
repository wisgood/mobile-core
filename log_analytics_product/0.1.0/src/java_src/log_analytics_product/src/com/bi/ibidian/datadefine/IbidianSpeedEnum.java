package com.bi.ibidian.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @author wangzg
 * 
 * @DESC: original fields number is 8
 *        transform fields number is 6
 * 
 */
public enum IbidianSpeedEnum
{
	O_TIMESTAMP,
	O_IP,
	O_MAC,
	O_USERID,
	O_FPC,
	O_URL,
	O_REFER,
	O_VTIME,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		IbidianSpeedEnum[] fields = IbidianSpeedEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (IbidianSpeedEnum singleEnum : fields)
		{
			fieldsSet.add(singleEnum.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		IbidianSpeedEnum[] fields = IbidianSpeedEnum.values();
		for (IbidianSpeedEnum singleEnum : fields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(IbidianSpeedEnum.class.getName(), fieldName);
	}

}
