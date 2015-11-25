package com.bi.ibidian.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @author wangzg
 * 
 * @DESC: original fields number is 12
 *        transform fields number is 6
 * 
 */
public enum IbidianPVEnum
{
	O_TIMESTAMP,
	O_IP,
	O_FCK,
	O_MAC,
	O_USERID,
	O_PAGENAME,
	O_BLOCKNAME,
	O_URL,
	O_REFER,
	O_ORDER,
	O_TIME,
	O_CORE,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		IbidianPVEnum[] fields = IbidianPVEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (IbidianPVEnum ibidianPVEnum : fields)
		{
			fieldsSet.add(ibidianPVEnum.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		IbidianPVEnum[] fields = IbidianPVEnum.values();
		for (IbidianPVEnum singleEnum : fields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(IbidianPVEnum.class.getName(), fieldName);
	}

}
