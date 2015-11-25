package com.bi.common.util;

import java.util.HashMap;
import java.util.Map;

public class PvListPageUtil {

	public static final String URL_SPLIT_SYMBOL = "/";

	public static final String TAB_SPLIT_SYMBOL = "\\.";

	public static final String TAB_TYPE_SPLIT_SYMBOL = "-";

	public static final int LIST_URL_TAB_INDEX = 5;

	public static final int LIST_URL_MIN_LENGTH = 4;

	public static final String DEFAULT_ERROR_NUM = "-999";

	public static final String DEFAULT_EMPTY_NUM = "0";

	public static Map<String, String> getURLListTabList(String urlStr) {

		Map<String, String> tabTypeMap = new HashMap<String, String>();

		if (null == urlStr || urlStr.isEmpty()) {
			tabTypeMap.put(DEFAULT_ERROR_NUM, DEFAULT_ERROR_NUM);
			return tabTypeMap;
		}

		String[] urlLevels = urlStr.split(URL_SPLIT_SYMBOL, -1);

		if (urlLevels.length >= LIST_URL_MIN_LENGTH
				&& urlLevels[LIST_URL_MIN_LENGTH - 1].trim().equals("list")) {
			if (urlLevels.length > LIST_URL_TAB_INDEX) {
				String urlTabList = urlLevels[LIST_URL_TAB_INDEX].trim();
				if (urlTabList.contains("?")) {
					String[] urlTabPart = urlTabList.split("\\?");
					if (urlTabPart.length > 0) {
						urlTabList = urlTabPart[0].trim();
					}
				}
				if (urlTabList.contains(".")) {
					String[] tabTypePairs = urlTabList.split(TAB_SPLIT_SYMBOL,
							-1);

					if (tabTypePairs.length > 0) {
						for (String tabType : tabTypePairs) {

							if (tabType.contains(TAB_TYPE_SPLIT_SYMBOL)) {

								String[] tabPair = tabType
										.split(TAB_TYPE_SPLIT_SYMBOL);

								if (tabPair.length > 1) {
									tabTypeMap.put(tabPair[0].trim(),
											tabPair[1].trim());
								}
							}
						}
						if (tabTypeMap.size() <= 0) {
							tabTypeMap
									.put(DEFAULT_ERROR_NUM, DEFAULT_ERROR_NUM);
						}
					} else {
						tabTypeMap.put(DEFAULT_ERROR_NUM, DEFAULT_ERROR_NUM);
					}
				} else {
					if (urlTabList.contains(TAB_TYPE_SPLIT_SYMBOL)) {
						String[] tabPair = urlTabList
								.split(TAB_TYPE_SPLIT_SYMBOL);
						if (tabPair.length > 1) {
							tabTypeMap
									.put(tabPair[0].trim(), tabPair[1].trim());
						}
					} else {
						tabTypeMap.put(DEFAULT_EMPTY_NUM, DEFAULT_EMPTY_NUM);
					}
				}
			} else {
				if (urlLevels.length == LIST_URL_TAB_INDEX) {
					tabTypeMap.put(DEFAULT_EMPTY_NUM, DEFAULT_EMPTY_NUM);
				} else {
					tabTypeMap.put(DEFAULT_ERROR_NUM, DEFAULT_ERROR_NUM);
				}
			}
		} else {
			tabTypeMap.put(DEFAULT_ERROR_NUM, DEFAULT_ERROR_NUM);
		}
		return tabTypeMap;
	}
}
