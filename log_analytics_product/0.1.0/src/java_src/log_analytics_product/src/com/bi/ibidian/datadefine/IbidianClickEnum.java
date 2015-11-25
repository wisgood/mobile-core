package com.bi.ibidian.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.ibidian.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

/**
 * 
 * @author wangzg
 * 
 * @DESC: original fields number is 11
 *        transform fields number is 6
 * 
 */
public enum IbidianClickEnum
{
	O_TIMESTAMP,
	O_IP,
	O_MAC,
	O_USERID,
	O_FCK,
	O_TIME,
	O_REFER,
	O_PAGENAME,
	O_BLOCKNAME,
	O_SORT,
	O_URL,
	T_DATEID,
	T_HOURID,
	T_IP,
	T_PROVINCEID,
	T_CITYID,
	T_ISPID;

	public static boolean containsField(String fieldName)
	{
		IbidianClickEnum[] fields = IbidianClickEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (IbidianClickEnum ibidianClickEnum : fields)
		{
			fieldsSet.add(ibidianClickEnum.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		IbidianClickEnum[] fields = IbidianClickEnum.values();
		for (IbidianClickEnum singleEnum : fields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(IbidianClickEnum.class.getName(), fieldName);
	}

}
