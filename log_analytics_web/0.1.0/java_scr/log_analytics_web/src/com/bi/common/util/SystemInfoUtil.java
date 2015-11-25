package com.bi.common.util;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bi.common.init.ConstantEnum;

public class SystemInfoUtil {

	private int compareVersions(String strVersion1, String strVersion2) {
		String[] aVersion1 = strVersion1.trim().split("\\.");
		String[] aVersion2 = strVersion2.trim().split("\\.");
		int cmpLength = 0;
		int verType = 0;
		if (aVersion1.length >= aVersion2.length) {
			cmpLength = aVersion1.length;
			verType = 1;
		} else if (aVersion1.length < aVersion2.length) {
			cmpLength = aVersion2.length;
			verType = 2;
		}
		String[] bVersion = new String[cmpLength];
		if (verType == 1) {
			for (int i = 0; i < cmpLength; i++) {
				if (i < aVersion2.length) {
					bVersion[i] = aVersion2[i];
				} else {
					bVersion[i] = "0";
				}
			}
			for (int j = 0; j < bVersion.length; j++) {
				if (Integer.parseInt(aVersion1[j]) < Integer
						.parseInt(bVersion[j])) {
					return -1;
				} else if (Integer.parseInt(aVersion1[j]) > Integer
						.parseInt(bVersion[j])) {
					return 1;
				}
			}
		} else if (verType == 2) {
			for (int i = 0; i < cmpLength; i++) {
				if (i < aVersion1.length) {
					bVersion[i] = aVersion1[i];
				} else {
					bVersion[i] = "0";
				}
			}
			for (int j = 0; j < bVersion.length; j++) {
				if (Integer.parseInt(bVersion[j]) < Integer
						.parseInt(aVersion2[j])) {
					return -1;
				} else if (Integer.parseInt(bVersion[j]) > Integer
						.parseInt(aVersion2[j])) {
					return 1;
				}
			}
		}
		return 0;
	}

	@SuppressWarnings("unused")
	public static Map<ConstantEnum, String> getBroAndOSInfo(String strSysInfo) {
		if (strSysInfo.isEmpty()) {
			return null;
		}
		Map<ConstantEnum, String> sysAndbroMap = new WeakHashMap<ConstantEnum, String>();
		String BrowType = null;
		String sysType = null;
		String strInputSysInfo = strSysInfo.toLowerCase().trim();
		int isOpera = strInputSysInfo.indexOf("opera");
		int isKHTML = strInputSysInfo.indexOf("khtml");
		boolean isIE = false;
		boolean isMoz = false;
		if (isOpera > -1) {
			// User-Agent:Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; -?)
			// Opera 12.60
			Pattern pattern = Pattern
					.compile("opera\\s*\\/?([0-9]+(\\.[0-9]+)*)"); // .*[0-9]+//.[0-9]+
			Matcher mather = pattern.matcher(strInputSysInfo);
			if (mather.find()) {
				BrowType = mather.group();
			}
		} else if (isKHTML > -1 || strInputSysInfo.indexOf("konqueror") > -1
				|| strInputSysInfo.indexOf("applewebkit") > -1) {
			if (isKHTML > -1) {
				int isSafari = strInputSysInfo.indexOf("applewebkit");
				int isKonq = strInputSysInfo.indexOf("konqueror");

				if (strInputSysInfo.contains("chrome")) {
					Pattern pattern = Pattern
							.compile("chrome\\s*\\/*\\s*(\\s*\\d+(\\.\\d+)*)");
					Matcher mather = pattern.matcher(strInputSysInfo);
					if (mather.find()) {
						BrowType = mather.group();
					}
				}
				// MacOS X的默认浏览器,基于KHTML，参考前面给出的userAgent
				else if (isSafari > -1) {
					Pattern pattern = Pattern
							.compile("safari\\s*\\/?(\\s*\\d+(\\.\\d+)*)"); // .*(//d+(?://.//s*)?)
					Matcher mather = pattern.matcher(strInputSysInfo);
					if (mather.find()) {
						BrowType = mather.group();
					}
				}
				// 判断是否是Konqueror浏览器,Unix下的浏览器,基于KHTML
				else if (isKonq > -1) {
					Pattern pattern = Pattern
							.compile("konqueror\\s*\\/?(\\s*\\d+(\\.\\d+)*)"); // \\s*\\/(//s+(?://.//d+(?://.//d)?)?)
					Matcher mather = pattern.matcher(strInputSysInfo);
					if (mather.find()) {
						BrowType = mather.group();
					}
				}
			}
		} else if (strInputSysInfo.indexOf("compatible") > -1
				&& strInputSysInfo.indexOf("msie") > -1 && isOpera == -1) {
			isIE = true;
			if (isIE) {
				Pattern pattern = Pattern.compile("msie\\s*(\\d+(\\.\\d+)*)"); //
				Matcher mather = pattern.matcher(strInputSysInfo);
				if (mather.find()) {
					BrowType = mather.group();
				}
			}
		} else if (strInputSysInfo.indexOf("gecko") > -1 && isKHTML == -1) {
			Pattern pattern = Pattern
					.compile("firefox\\s*\\/?(\\s*\\d+(\\.\\d+)*)");
			Matcher mather = pattern.matcher(strInputSysInfo);
			if (mather.find()) {
				BrowType = mather.group();
			}
		}

		boolean isWinME = false;
		boolean isWin2K = false;
		boolean isWinXP = false;
		if (strInputSysInfo.indexOf("win98") > -1
				|| strInputSysInfo.indexOf("windows 98") > -1) {
			sysType = "windows 98";
		} else if (strInputSysInfo.indexOf("win 9x 4.90") > -1
				|| strInputSysInfo.indexOf("windows me") > -1) {
			sysType = "windows me";
			isWinME = true;
		} else if (strInputSysInfo.indexOf("windows nt 5.0") > -1
				|| strInputSysInfo.indexOf("windows 2000") > -1) {
			sysType = "windows 2000";
			isWin2K = true;
		} else if (strInputSysInfo.indexOf("windows nt 5.1") > -1
				|| strInputSysInfo.indexOf("windows xp") > -1) {
			sysType = "windows XP";
			isWinXP = true;
		} else if (strInputSysInfo.indexOf("windows") > -1) {
			// System.out.print("is windows!!");
			Pattern pattern = Pattern
					.compile("windows\\s*(nt)*\\s*\\d+(\\.\\d+)*"); // (?:\\.\\d+)(\\d+\\.\\d+)
																	// \\s*nt\\s*
			Matcher mather = pattern.matcher(strInputSysInfo);
			if (mather.find()) {
				sysType = mather.group();
			}
		} else if (strInputSysInfo.indexOf("winnt") > -1
				|| strInputSysInfo.indexOf("windowsnt") > -1
				|| strInputSysInfo.indexOf("winnt4.0") > -1
				|| strInputSysInfo.indexOf("windows nt 4.0") > -1
				&& (!isWinME && !isWin2K && !isWinXP)) {
			sysType = "windows nt4";
		} else if (strInputSysInfo.indexOf("max_68000") > -1
				|| strInputSysInfo.indexOf("68k") > -1) {
			sysType = "Max_68000";
		} else if (strInputSysInfo.indexOf("mac_powerpc") > -1
				|| strInputSysInfo.indexOf("ppc") > -1) {
			sysType = "Mac_PowerPC";
		} else if (strInputSysInfo.indexOf("mac os") > -1) {
			sysType = "MAC OS";
		} else if (strInputSysInfo.indexOf("sunos") > -1) {
			sysType = "SunOS";
			Pattern pattern = Pattern
					.compile("sunos\\s*(\\d+\\.\\d+(?:\\.\\d+)?)");
			Matcher mather = pattern.matcher(strInputSysInfo);
			if (mather.find()) {
				BrowType = mather.group();
			}
		}
		if (sysType == null) {
			sysType = "Unknown";
		}
		if (BrowType == null) {
			BrowType = "Unknown";
		}
		BrowType = BrowType.replaceAll("\\/|\\s", "");
		sysType = sysType.replaceAll("\\s", "");

		sysAndbroMap.put(ConstantEnum.BROWSE_INFO, BrowType);
		sysAndbroMap.put(ConstantEnum.SYSTEM_INFO, sysType);
		return sysAndbroMap;
	}
}
