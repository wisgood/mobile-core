package com.bi.mobile.comm.util;

import org.apache.log4j.Logger;

import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;

public class SidFormatMobileUtil {
//	private static Logger logger = Logger.getLogger(SidFormatMobileUtil.class
//			.getName());

	public static int getSid(String qudaoInfo,
			AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO) {
		int qudaoId = 1;
		try {
			qudaoId = Integer.parseInt(qudaoInfo);
			qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
//			logger.error(e.getMessage(), e.getCause());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
//			logger.error(e.getMessage(), e.getCause());
		}
		return qudaoId;
	}

	public static int getSidByEnum(String[] splitSts,
			AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO, String enumClassStr) {

		int qudaoId = 1;
		try {
			Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
			String qudaoInfo = splitSts[Enum.valueOf(logEnum, "SID").ordinal()];
			qudaoId = Integer.parseInt(qudaoInfo);
			qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
//			logger.error(e.getMessage(), e.getCause());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
//			logger.error(e.getMessage(), e.getCause());
		}
		return qudaoId;
	}

}
