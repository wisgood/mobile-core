package com.bi.comm.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMACFormatUtil {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMacFormat() {
		
		
		System.out.println(MACFormatUtil.macFormat("EC:85:2F:49:ED:5A"));
		
	}
	@Test
	public void testMacFormatRes() {
		
		try {
			MACFormatUtil.isCorrectMac("(NULL)");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
