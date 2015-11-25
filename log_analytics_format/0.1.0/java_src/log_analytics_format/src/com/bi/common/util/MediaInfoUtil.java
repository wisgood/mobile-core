package com.bi.common.util;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.constant.DMMediaTypeEnum;
import com.bi.common.constant.ConstantEnum;

public class MediaInfoUtil {

	private static final String MOBILE_WEB_LONG_MEDIA_ID = "mediaid";

	private static final String MOBILE_WEB_VIDEO_ID = "mid";

	private static final String MOBILE_WEB_MEDIA_NUMBER = "number";

	private static final String WEB_MEDIA_DEFAULT = "-999";

	public static Map<String, String> getMediaType(String MediaInfoStr) {

		Map<String, String> MediaIDMap = new WeakHashMap<String, String>();

		if (MediaInfoStr.isEmpty() || MediaInfoStr == null) {
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap
					.put(DMMediaTypeEnum.MEDIA_TYPE.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(),
					WEB_MEDIA_DEFAULT);
			MediaIDMap
					.put(DMMediaTypeEnum.MEDIA_NAME.name(), WEB_MEDIA_DEFAULT);
		}

		String DecodedMediaInfo = null;

		try {
			DecodedMediaInfo = StringDecodeFormatUtil
					.decodeCodedStr(
							StringDecodeFormatUtil.decodeFromHex(MediaInfoStr),
							"utf-8");
		} catch (Exception e) {

			DecodedMediaInfo = "";
			// logger.error(e.getMessage(), e.getCause());
		}
		String[] mediaInfoStrs = DecodedMediaInfo.split("\\|");

		String playId = WEB_MEDIA_DEFAULT;

		String mediaType = WEB_MEDIA_DEFAULT;

		String subjectType = WEB_MEDIA_DEFAULT;

		String mediaName = "";

		if (mediaInfoStrs.length >= 1) {

			String playType = mediaInfoStrs[0];

			if (playType.toLowerCase().contains("media")) {
				playId = "1";
			} else if (playType.toLowerCase().contains("video")
					|| playType.toLowerCase().contains("ugc")) {
				playId = "2";
			} else if (playType.toLowerCase().contains("live")) {
				playId = "3";
			}
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), playId);

			if (mediaInfoStrs.length >= 2) {
				if (!mediaInfoStrs[1].trim().isEmpty()) {
					mediaType = mediaInfoStrs[1].trim();
				}
			}
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_TYPE.name(), mediaType);
			if (mediaInfoStrs.length >= 3) {
				subjectType = mediaInfoStrs[2].trim();
				if (mediaInfoStrs.length >= 4) {
					mediaName = mediaInfoStrs[3].trim();
				}
			}
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(), subjectType);
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_NAME.name(), mediaName);
		} else {
			MediaIDMap.put(DMMediaTypeEnum.PLAY_TYPE.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap
					.put(DMMediaTypeEnum.MEDIA_TYPE.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.SUBJECT_TYPE.name(),
					WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_NAME.name(), "");
		}
		return MediaIDMap;
	}

	public static Map<String, String> getMediaID(String MediaIDStr) {

		Map<String, String> MediaIDMap = new WeakHashMap<String, String>();

		if (MediaIDStr.isEmpty() || MediaIDStr == null) {
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(),
					WEB_MEDIA_DEFAULT);
		}

		String[] mediaIDStrs = MediaIDStr.split("\\|");

		if (mediaIDStrs.length >= 1) {

			String mediaId = WEB_MEDIA_DEFAULT;

			String serialId = WEB_MEDIA_DEFAULT;

			String tagId = WEB_MEDIA_DEFAULT;

			String relateId = WEB_MEDIA_DEFAULT;

			if ((!mediaIDStrs[0].isEmpty())
					&& (true == WebCheckUtil.checkIsNum(mediaIDStrs[0]))) {
				mediaId = mediaIDStrs[0].trim();
			}
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), mediaId);

			if (mediaIDStrs.length >= 2) {
				if ((!mediaIDStrs[1].isEmpty())
						&& (true == WebCheckUtil.checkIsNum(mediaIDStrs[1]))) {
					serialId = mediaIDStrs[1].trim();
				}

			}
			if (mediaIDStrs.length >= 3) {
				tagId = mediaIDStrs[2].trim();
			}
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), tagId);
			if (mediaIDStrs.length >= 4) {
				relateId = mediaIDStrs[3].trim();
			}
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), serialId);
			MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(), relateId);

		} else {
			MediaIDMap.put(DMMediaTypeEnum.MEDIA_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.SEARIA_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.TAG_ID.name(), WEB_MEDIA_DEFAULT);
			MediaIDMap.put(DMMediaTypeEnum.RELATE_VIDEO_ID.name(),
					WEB_MEDIA_DEFAULT);
		}
		return MediaIDMap;
	}

	public static boolean getUserType(String fckStr, String nowDayStr) {

		if (fckStr.isEmpty() || fckStr == null) {
			return false;
		}
		if (nowDayStr.isEmpty() || nowDayStr == null) {
			return false;
		}
		String fckTimeStr = null;
		if (fckStr.length() == 15) {
			fckTimeStr = fckStr.substring(0, 10);
			Pattern pattern = Pattern.compile("[0-9]*"); // .*[0-9]+//.[0-9]+
			Matcher matcher = pattern.matcher(fckTimeStr);
			if (matcher.matches()) {
				Map<ConstantEnum, String> formatTimeMap = TimestampFormatUtil
						.formatTimestamp(fckTimeStr);
				if (formatTimeMap != null) {
					String fckDateID = formatTimeMap.get(ConstantEnum.DATE_ID);
					if (fckDateID.equalsIgnoreCase(nowDayStr)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public static Map<ConstantEnum, String> getMobileWebMediaInfo(String urlStr) {

		Map<ConstantEnum, String> mediaInfoMap = new WeakHashMap<ConstantEnum, String>();

		if (urlStr.isEmpty() || null == urlStr)
			return mediaInfoMap;

		String[] urlFields = urlStr.trim().split("\\&", -1);
		for (String urlField : urlFields) {
			int midIndex = urlField.indexOf(MOBILE_WEB_LONG_MEDIA_ID);
			int vidIndex = urlField.indexOf(MOBILE_WEB_VIDEO_ID);

			if (midIndex >= 0) {
				String mediaId = urlField.substring(midIndex
						+ MOBILE_WEB_LONG_MEDIA_ID.length() + 1);
				mediaInfoMap.put(ConstantEnum.MEIDA_ID, mediaId);
			}
			if (vidIndex >= 0) {
				String mediaId = urlField.substring(vidIndex
						+ MOBILE_WEB_VIDEO_ID.length() + 1);
				mediaInfoMap.put(ConstantEnum.MEIDA_ID, mediaId);
			}
		}
		return mediaInfoMap;
	}
}