package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

public enum PlayerBuffEnum
{
	/*
	 * DATE_ID,日期ID
	 * HOUR_ID,小时ID
	 * VERSION_ID,版本ID
	 * PROVINCE_ID,省ID
	 * CITY_ID,城市ID
	 * ISP_ID,运营商ID
	 * MAC_FORMAT, 经过验证的MAC地址
	 * BS_FORMAT, 缓冲时间
	 * SID, SESSION ID
	 * CLIENTIP, 客户端IP
	 * TIMESTAMP, 接收上报时间(rt)：unix时间戳
	 * PGID, package_id，每个客户端独立编号
	 * PVS, 工作模式
	 * BS, Buffer Size
	 * IH, 任务infohash
	 * MAC, MAC地址 
	 * VV, 客户端版本,点分十进制
	 * PROVINCE, 省ID
	 * CITY, 城市ID
	 * ISP, 运营商ID
	 * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_buff
	 */

	DATE_ID,
	HOUR_ID,
	VERSION_ID,
	PROVINCE_ID,
	CITY_ID,
	ISP_ID,
	MAC_FORMAT,
	BS_FORMAT,
	SID,
	CLIENTIP,
	TIMESTAMP,
	PGID,
	PVS,
	BS,
	IH,
	MAC,
	VV,
	PROVINCE,
	CITY,
	ISP;

	public static boolean containsField(String fieldName)
	{
		PlayerBuffEnum[] enumFields = PlayerBuffEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (PlayerBuffEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		PlayerBuffEnum[] enumFields = PlayerBuffEnum.values();
		for (PlayerBuffEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(PlayerBuffEnum.class.getName(), fieldName);
	}

}
