package com.bi.extend.util;

public class SpecialVersionRecomposeFormatMobileUtil {

	public static String[] ecomposeBySpecialVersion(String[] splitSts,
			int indexSid) throws ClassNotFoundException {
		int indexVer = indexSid - 1;
		String versionInfo = splitSts[indexVer];
		if ("1.2.0.2".equalsIgnoreCase(versionInfo)
				|| "1.2.0.1".equalsIgnoreCase(versionInfo)) {
			splitSts = new String[splitSts.length];
			for (int i = 0; i < indexVer; i++) {
				splitSts[i] = splitSts[i];
			}
			splitSts[indexVer] = versionInfo;
			for (int i = indexVer; i < splitSts.length; i++) {
				if (i < splitSts.length - 1) {
					splitSts[i + 1] = splitSts[i];
				} else {
					splitSts[i] = splitSts[i];
				}
			}
		}
		return splitSts;
	}
}
