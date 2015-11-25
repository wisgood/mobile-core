package com.bi.common.util;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.log4j.Logger;

import com.bi.common.dm.constant.DMMediaTypeEnum;
import com.bi.common.init.ConstantEnum;


public class MediaInfoUtil {
	private static Logger logger = Logger.getLogger(MediaInfoUtil.class
			.getName());

	public static Map<String, String> getMediaType(String MediaInfoStr) {
		Map<String, String> MediaIDMap = new WeakHashMap<String, String>();
		if (MediaInfoStr.isEmpty() || MediaInfoStr == null) {
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_NAME.name(), "");
		}
		String DecodedMediaInfo = null;
		try {
			DecodedMediaInfo = StringDecodeFormatUtil
					.decodeCodedStr(
							StringDecodeFormatUtil.decodeFromHex(MediaInfoStr),
							"utf-8");
		} catch (Exception e) {
			DecodedMediaInfo = "";
			 //logger.error(e.getMessage(), e.getCause());
		}
		String[] mediaInfoStrs = DecodedMediaInfo.split("\\|");
		if (mediaInfoStrs.length >= 3) {
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), mediaInfoStrs[0]);
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_TYPE.name(), mediaInfoStrs[1]);
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(),
					mediaInfoStrs[2]);
			if (mediaInfoStrs.length >= 4){
				MediaIDMap.put(DMMediaTypeEnum.MEDIA_NAME.name(), mediaInfoStrs[3]);
			}
		} else {
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_NAME.name(), "");
		}
		return MediaIDMap;
	}

	public static Map<String, String> getMediaID(String MediaIDStr) {
		Map<String, String> MediaIDMap = new WeakHashMap<String, String>();
		if (MediaIDStr.isEmpty() || MediaIDStr == null) {
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(), "");
		}
		String[] mediaIDStrs = MediaIDStr.split("\\|");
		if (mediaIDStrs.length >= 3) {
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), mediaIDStrs[0]);
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), mediaIDStrs[1]);
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), mediaIDStrs[2]);
			if (mediaIDStrs.length >= 4){
				MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(),
						mediaIDStrs[3]);
			}
		} else {
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), "");
			MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(), "");
		}
		return MediaIDMap;
	}
	
	public static boolean getUserType(String fckStr, String nowDayStr){
		if(fckStr.isEmpty()||fckStr == null){
			return false;
		}
		if(nowDayStr.isEmpty()||nowDayStr == null){
			return false;
		}
		String fckTimeStr = null;
		if(fckStr.length() > 14){
			fckTimeStr = fckStr.substring(0, 10);
			Map<ConstantEnum, String> formatTimeMap = TimestampFormatUtil
					.formatTimestamp(fckTimeStr);
			if(formatTimeMap != null){
				String fckDateID = formatTimeMap.get(ConstantEnum.DATE_ID);
				if(fckDateID.equalsIgnoreCase(nowDayStr)){
					return true;
				}
			}
		}
		return false;
	}
}
