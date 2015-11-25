package com.bi.mobile.dm.pojo.dao;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

public class TestDMIPRuleDAOImpl {
	private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
	@Before
	public void setUp() throws Exception {
		this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
		this.dmIPRuleDAO.parseDMObj(new File("conf/ip_table"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseDMObj() {
		
	}

	@Test
	public void testGetDMOjb() {
		try {
			System.out.println(this.dmIPRuleDAO.getDMOjb(3740283342l));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
