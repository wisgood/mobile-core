package com.bi.mobile.pojo;



import org.junit.Test;

import com.bi.mobile.exit.format.dataenum.ExitFormatEnum;

public class TestExitETLEnum {

	@Test
	public void testTIMESTAMPIndex() {
		System.out.println(ExitFormatEnum.TIMESTAMP.ordinal());
	}

	@Test
	public void testLe(){
		
	
		System.out.println("1365811964,111.173.194.22,iPhone_6.0_iPhone4.1,B4:F0:AB:8A:85:6A,1,3372897.750000,-1,1.2.0.1,0".split(",").length);
	}
}
