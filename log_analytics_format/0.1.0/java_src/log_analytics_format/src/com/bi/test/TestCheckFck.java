package com.bi.test;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMInterURLImpl;
import com.bi.common.dimprocess.DMKeywordRuleDAOImpl;
import com.bi.common.dimprocess.DMOuterURLRuleImpl;
import com.bi.common.dimprocess.DMURLListPageDAOImpl;
import com.bi.common.dimprocess.DMURLListTabTypeDAOImpl;
import com.bi.common.logenum.PvEnum;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.MediaInfoUtil;
import com.bi.common.util.DMIPRuleDAOImpl;
//import com.bi.common.dm.pojo.dao.DMInterURLDAOObj;
import com.bi.common.util.PvListPageUtil;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.SystemInfoUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.bi.common.util.WebCheckUtil;

public class TestCheckFck {

	static final String FIELD_TAB_SEPARATOR = "\t";

	static final String FIELD_COMM_SEPARATOR = ",";

	static final String DEFAULT_NEGATIVE_NUM = "-999";

	static final String DEFAULT_TAB_NUM = "0";

	private static final String urlSeparator = "http://";

	private String dateId = null;

	private static AbstractDMDAO<String, Map<ConstantEnum, String>> dmOuterURLRuleDAO = new DMOuterURLRuleImpl<String, Map<ConstantEnum, String>>();

	private static AbstractDMDAO<String, Map<ConstantEnum, String>> dmKeywordRuleDAO = new DMKeywordRuleDAOImpl<String, Map<ConstantEnum, String>>();

	private static DMInterURLImpl dmInterURLRuleDAO = new DMInterURLImpl();

	public static String getPVFormatStr(String originalData) throws Exception {

		dmOuterURLRuleDAO.parseDMObj(new File(
				"e:/dev/data/2_web/0_new_format/dm_outer_url_new"));
		dmInterURLRuleDAO.parseDMObj(new File(
				"e:/dev/data/2_web/0_new_format/dm_inter_url"));
		dmKeywordRuleDAO.parseDMObj(new File(
				"e:/dev/data/2_web/0_new_format/dm_url_keyword_2"));

		StringBuilder pvFormatStr = new StringBuilder();
		String[] rawsplitStr = originalData.split("\t", -1);

		String buildData = originalData;
		if (rawsplitStr.length < PvEnum.TA.ordinal() + 1) {
			StringBuilder rawData = new StringBuilder(originalData);
			for (int i = rawsplitStr.length; i < PvEnum.TA.ordinal() + 1; i++) {
				rawData.append("" + StringFormatUtil.TAB_SEPARATOR
						+ DEFAULT_NEGATIVE_NUM);
			}
			buildData = rawData.toString();
		}
		String[] splitStr = buildData.split("\t", -1);

		try {
			String dateId="20140113";
			String timestampInfo = splitStr[PvEnum.TIMESTAMP.ordinal()].trim();
			String dateIdAndhourId = TimestampFormatNewUtil.formatTimestamp(
					timestampInfo, dateId);
			// String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
			// String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

			String versionInfo = WebCheckUtil.checkField(
					splitStr[PvEnum.VERSION.ordinal()], "0.0.0.0");
			long versionId = -1;
			versionId = IPFormatUtil.ip2long(versionInfo);

			String fckStr = splitStr[PvEnum.FCK.ordinal()];
			String fckInfo = "";
			if (true == WebCheckUtil.checkFck(fckStr)) {
				fckInfo = fckStr;
			} else {
				fckInfo = DefaultFieldValueEnum.fckDefault.getValueStr();
			}
			String userTypeInfo = "0";
			if (MediaInfoUtil.getUserType(fckStr, dateId) == true) {
				userTypeInfo = "1";
			}
			 String clientflagInfo = "1";

			//String clientflagInfo = WebCheckUtil.checkField(
			//		splitStr[PvEnum.CLIENTFLAG.ordinal()], "0");

			String clientflag = WebCheckUtil.checkClientFlag(clientflagInfo);

			String qudaoId = "0";
			if (WebCheckUtil.checkIsNum(splitStr[PvEnum.QUDAO_ID.ordinal()]) == true) {
				qudaoId = "1";
			}

			String urlInfoRegion = splitStr[PvEnum.URL.ordinal()];
			String urlInfo = StringDecodeFormatUtil.decodeCodedStr(
					urlInfoRegion, "utf-8").replaceAll("\\s+", "");
			if (urlInfo.isEmpty() || null == urlInfo) {
				urlInfo = DEFAULT_NEGATIVE_NUM;
			}

			java.util.Map<ConstantEnum, String> URLTypeMap = dmInterURLRuleDAO
					.getDMOjb(urlInfo);

			String urlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
					.trim();
			String urlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID)
					.trim();
			String urlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
					.trim();
			String urlKeyword = "";
			String urlPage = DEFAULT_NEGATIVE_NUM;
			if (URLTypeMap.size() > 3) {
				urlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD).trim();
				urlPage = URLTypeMap.get(ConstantEnum.URL_PAGE).trim();
			}

			String referUrlInfoRegion = splitStr[PvEnum.REFERURL.ordinal()];
			String referUrlInfoStr = referUrlInfoRegion.replaceAll("\\s+", "");

			String referUrlFirstId = DEFAULT_NEGATIVE_NUM;
			String referUrlSecondId = DEFAULT_NEGATIVE_NUM;
			String referUrlThirdId = DEFAULT_NEGATIVE_NUM;
			String referUrlKeyword = "";
			String referUrlInfo = referUrlInfoStr;
			String referUrlPage = DEFAULT_NEGATIVE_NUM;
			if (referUrlInfoStr.contains(urlSeparator)) {
				String referUrlInfoArr[] = referUrlInfoStr.split(urlSeparator);
				referUrlInfo = urlSeparator.concat(referUrlInfoArr[1]);
			}
			if (referUrlInfo.length() > 101) {
				referUrlInfo = referUrlInfo.substring(0, 100);
			}
			if ((referUrlInfo.contains(".funshion.com") && !referUrlInfo
					.contains("vas.funshion.com"))
					|| referUrlInfo.contains("news.smgbb.cn")) {
				URLTypeMap = dmInterURLRuleDAO.getDMOjb(referUrlInfo);
				referUrlFirstId = URLTypeMap.get(ConstantEnum.URL_FIRST_ID)
						.trim();
				referUrlSecondId = URLTypeMap.get(ConstantEnum.URL_SECOND_ID)
						.trim();
				referUrlThirdId = URLTypeMap.get(ConstantEnum.URL_THIRD_ID)
						.trim();
				if (URLTypeMap.size() > 3) {
					referUrlKeyword = URLTypeMap.get(ConstantEnum.URL_KEYWORD)
							.trim();
					referUrlPage = URLTypeMap.get(ConstantEnum.URL_PAGE).trim();
				}
			} else {
				java.util.Map<ConstantEnum, String> referUrlRuleMap =dmOuterURLRuleDAO
						.getDMOjb(referUrlInfo);

				referUrlFirstId = referUrlRuleMap
						.get(ConstantEnum.URL_FIRST_ID).trim();
				referUrlSecondId = referUrlRuleMap.get(
						ConstantEnum.URL_SECOND_ID).trim();
				referUrlThirdId = referUrlRuleMap
						.get(ConstantEnum.URL_THIRD_ID).trim();

				java.util.Map<ConstantEnum, String> keywordRuleMap = dmKeywordRuleDAO
						.getDMOjb(referUrlInfo);
				referUrlKeyword = keywordRuleMap.get(ConstantEnum.URL_KEYWORD)
						.trim();
			}

			// mac地址
			String macInfoStr = WebCheckUtil.checkField(
					splitStr[PvEnum.MAC.ordinal()], "0");
			String macInfo = MACFormatUtil.getCorrectMac(macInfoStr);
			String userAgentStr = splitStr[PvEnum.USERAGENT.ordinal()]
					.replaceAll("\\s+", "");

			java.util.Map<ConstantEnum, String> sysAndbroMap = SystemInfoUtil
					.getBroAndOSInfo(userAgentStr);
			String browseInfo = "";
			String systemInfo = "";
			if (userAgentStr != null && !"".equals(userAgentStr)) {
				browseInfo = sysAndbroMap.get(ConstantEnum.BROWSE_INFO).trim();
				systemInfo = sysAndbroMap.get(ConstantEnum.SYSTEM_INFO).trim();
			}
			String vTime = splitStr[PvEnum.VTIME.ordinal()];
			if (WebCheckUtil.checkIsNum(vTime) == false) {
				vTime = DEFAULT_NEGATIVE_NUM;
			}
			String sessionId = WebCheckUtil
					.checkField(splitStr[PvEnum.SESSIONID.ordinal()]
							.replaceAll("\\s+", ""), DEFAULT_NEGATIVE_NUM);

			String userId = splitStr[PvEnum.USERID.ordinal()];
			if (WebCheckUtil.checkIsNum(userId) == false) {
				userId = "0";
			}

			String pvId = splitStr[PvEnum.PVID.ordinal()];
			if (WebCheckUtil.checkIsNum(pvId) == false) {
				pvId = DEFAULT_NEGATIVE_NUM;
			}

			String pvStep = splitStr[PvEnum.STEP.ordinal()];
			if (WebCheckUtil.checkIsNum(pvStep) == false) {
				pvStep = "0";
			}
			String pvSeStep = splitStr[PvEnum.SESTEP.ordinal()];
			if (WebCheckUtil.checkIsNum(pvSeStep) == false) {
				pvSeStep = DEFAULT_NEGATIVE_NUM;
			}
			String seIdCount = splitStr[PvEnum.SEIDCOUNT.ordinal()];
			if (WebCheckUtil.checkIsNum(seIdCount) == false) {
				seIdCount = DEFAULT_NEGATIVE_NUM;
			}
			String proID = splitStr[PvEnum.PROTOCOL.ordinal()];
			if (WebCheckUtil.checkIsNum(proID) == false) {
				proID = "0";
			}
			String rproID = splitStr[PvEnum.RPROTOCOL.ordinal()];
			if (WebCheckUtil.checkIsNum(rproID) == false) {
				rproID = "0";
			}
			pvFormatStr.append(dateIdAndhourId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append("0"
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append("0"
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append("0"
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(clientflag
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(qudaoId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(versionId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append("0"
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(macInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(fckInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(sessionId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(userId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(userTypeInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(urlInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(urlFirstId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(urlSecondId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(urlThirdId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(urlKeyword
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(urlPage
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(referUrlInfoStr
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(referUrlFirstId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(referUrlSecondId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(referUrlThirdId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(referUrlKeyword
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(referUrlPage
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(browseInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(systemInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(vTime
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(timestampInfo
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(proID
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(rproID
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr
					.append(splitStr[PvEnum.FPC.ordinal()].replaceAll("\\s+",
							"") + UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(pvId
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr
					.append(splitStr[PvEnum.CONFIG.ordinal()].replaceAll(
							"\\s+", "")
							+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr
					.append(splitStr[PvEnum.PAGE_TYPE.ordinal()].replaceAll(
							"\\s+", "")
							+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(userAgentStr
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(pvStep
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(pvSeStep
					+ UtilComstrantsEnum.tabSeparator.getValueStr());
			pvFormatStr.append(seIdCount
					+ UtilComstrantsEnum.tabSeparator.getValueStr());

			pvFormatStr.append(splitStr[PvEnum.TA.ordinal()].trim() + "");

		} catch (Exception e) {
			System.out.println(originalData);
			// logger.error(e.getMessage(), e.getCause());
		}
		return pvFormatStr.toString();
	}
	

	public static void main(String[] args) throws Exception{
		
		//DMInterURLDAOObj urlSample = new DMInterURLDAOObj();
/*		urlSample.parseDMObj(new File("e:/dev/data/pv_play_newlog/url_test3.txt"));
		String strTest = "video%7Cent%7C%u7535%u5F71%u9884%u544A%7C%u6D77%u5929%u76DB%u5BB4%uFF1A%u5BCC%u4E8C%u4EE3%u7231%u4E0A%u5916%u56F4%u5973%20%u6DF1%u5EA6%u8868%u767D%u5374%u906D%u62D2";
		String strTest2 = "http://fs.funshion.com/search/media?rec=1&sall=&kt=&ta=uoc&word=%E9%93%81%E8%A1%80%E7%8E%AB%E7%91%B0\n\r\n\t\n";
		String strTest3 = "http%3A%2F%2Ftv.sogou.com%2Fplay%3Fw%3D06060500%26query%3D%25CC%25D2%25BB%25A8%25D0%25A1%25C3%25C3%2B6%26tq%3D%25CC%25D2%25BB%25A8%25D0%25A1%25C3%25C3%2B%25B5%25E7%25CA%25D3%25BE%25E7%26i%3D-1%26j%3D6%26st%3D6%26tvsite%3Dall";
		String strSourcUrl = strTest2;
		//String strTestTar = StringDecodeFormatUtil.decodeCodedStr(strTest3, "utf-8");
		//String strTestTar3 = StringDecodeFormatUtil.changeCharset(strTest, "utf-8","utf-8");
		String strTestTar2 = StringDecodeFormatUtil.decodeFromHex(strTest);
		//String strTestTar4 = StringDecodeFormatUtil.decodeCodedStr(strTest, "utf-8");
		
		String testStr = "we			test	it	last		we	got	that		utf,,,";
		String[] testOutStr = testStr.split("\t");
		
		
		
		Map<ConstantEnum, String> urlLevelMap = urlSample.getDMOjb(strTest);
		Iterator<Entry<ConstantEnum, String>> iter = urlLevelMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<ConstantEnum, String> entry = (Entry<ConstantEnum, String>)iter.next();
			ConstantEnum strThirdKey = entry.getKey();
			String strThirdVal = entry.getValue();
			System.out.print(strThirdKey + "\t");
			System.out.print(strThirdVal + "\n");
		}
		
		for(String teStr:testOutStr){
			System.out.println(teStr);
		}
		System.out.println(testOutStr.length);*/
		//*
		String testStr = "11383616624062wd7";
	
		/*String testTarStr = StringDecodeFormatUtil.decodeCodedStr(
				String testTarStr =  MediaInfoUtil.getMediaType(testStr);
				//, "utf-8");
		System.out.print(testTarStr+"\n");
		String testStr2 = "123||4569|";
		*/

		String strTest = "3	3	1389621504	183.63.32.171	1	1386336003438f0	047D7BE1D4B0	0	4_f_c	2.8.6.56	13896214968297e	4e587d4b-4f25-5f5d-cbdb-65954ed42711	subject_	http://www.funshion.com/subject/94554/	http://www.so.com/s?ie=utf-8&src=hao_360so&q=%E5%85%A8%E7%BE%8E%E8%B6%85%E6%A8%A1%E5%A4%A7%E8%B5%9B%E7%AC%AC%E5%8D%81%E4%B8%89	1024	2176	pagetype=undefined	Mozilla/5.0(WindowsNT6.1)AppleWebKit/537.1(KHTML,like Gecko)Chrome/21.0.1180.89 Safari/537.125	211		 |ykz3frzehycisctt8f06p7f465kda157wyi";
		String strtest2 = "55,pg,c,o,r,k,u,e,p,h";
		//AbstractDMDAO<String, String> urlSample = new ();
		String result = getPVFormatStr(strTest);
		System.out.print(result +"#" + "123");
		
		/*
		Map<String, String> urlLevelMap = PvListPageUtil.getURLListTabList(strTest);
		Iterator<Entry<String, String>> iter = urlLevelMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>)iter.next();
			String strThirdKey = entry.getKey();
			String strThirdVal = entry.getValue();
			System.out.print(strThirdKey + "\t");
			System.out.print(strThirdVal + "\n");
		}
		*/
		
	}
}
