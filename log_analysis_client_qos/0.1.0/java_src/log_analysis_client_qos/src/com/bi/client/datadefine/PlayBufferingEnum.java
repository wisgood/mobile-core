package com.bi.client.datadefine;

import java.util.HashSet;
import java.util.Set;

import com.bi.client.datadefine.CustomEnumNameSet.CustomEnumFieldNotFoundException;

public enum PlayBufferingEnum
{
	/**
	 * DATE_ID,日期ID
	 * HOUR_ID,小时ID
	 * VERSION_ID,版本ID
	 * PROVINCE_ID,省ID
	 * CITY_ID,城市ID
	 * ISP_ID,运营商ID
	 * MAC_FORMAT, 经过验证的MAC地址
	 * PRN_FORMAT,peer数,若上报为undef,则置-1
	 * SDN_FORMAT,peer数,若上报为undef,则置-1
	 * OK_FORMAT, 缓冲是否完成,做数据验证,只能是0或1,其他置为-1
	 * BT_FORMAT, 缓冲时间,做数据验证,只能是0或正整数,失败是为-1
	 * SID, Session ID
	 * CLIENTIP, 客户端IP
	 * TIMESTAMP, 接收上报时间(rt)：unix时间戳
	 * PGID, package_id，每个客户端独立编号
	 * PRN, 上报中的Peer Number, 后添加字段，之前版本置为undef
	 * PVS, 工作模式
	 * BT, 缓冲时间, 单位ms
	 * IH, 任务infohash
	 * MAC, MAC地址
	 * NT, NAT类型
	 * OK,　缓冲是否完成标志
	 * SDN, 上报中的Seed Number, 后添加字段，之前版本置为undef
	 * TT, 当前下载速率
	 * APCT, first act peer cost time
	 * BTLT, 任务下载种子耗时
	 * FRCT, first request cost time
	 * TPCT, first temp peer cost time
	 * VV, 客户端版本,点分十进制
	 * PROVINCE, 省ID
	 * CITY, 城市ID
	 * ISP, 运营商ID
	 * 详情请见http://redmine.funshion.com/redmine/projects/data-analysis/wiki/Play_buffering
	 */

	DATE_ID,
	HOUR_ID,
	VERSION_ID,
	PROVINCE_ID,
	CITY_ID,
	ISP_ID,
	MAC_FORMAT,
	PRN_FORMAT,
	SDN_FORMAT,
	OK_FORMAT,
	BT_FORMAT,
	SID,
	CLIENTIP,
	TIMESTAMP,
	PGID,
	PRN,
	PVS,
	BT,
	IH,
	MAC,
	NT,
	OK,
	SDN,
	TT,
	APCT,
	BTLT,
	FRCT,
	TPCT,
	VV,
	PROVINCE,
	CITY,
	ISP;

	public static boolean containsField(String fieldName)
	{
		PlayBufferingEnum[] enumFields = PlayBufferingEnum.values();
		Set<String> fieldsSet = new HashSet<String>();
		for (PlayBufferingEnum field : enumFields)
		{
			fieldsSet.add(field.name());
		}
		return fieldsSet.contains(fieldName);
	}

	public static int getFieldOrder(String fieldName) throws CustomEnumFieldNotFoundException
	{
		PlayBufferingEnum[] enumFields = PlayBufferingEnum.values();
		for (PlayBufferingEnum singleEnum : enumFields)
		{
			if (fieldName.equals(singleEnum.name()))
			{
				return singleEnum.ordinal();
			}
		}
		throw new CustomEnumFieldNotFoundException(PlayBufferingEnum.class.getName(), fieldName);
	}

}
