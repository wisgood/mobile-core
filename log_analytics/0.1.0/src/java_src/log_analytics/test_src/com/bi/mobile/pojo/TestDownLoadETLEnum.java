package com.bi.mobile.pojo;


import org.junit.Test;

import com.bi.mobile.download.format.dataenum.DownLoadFormatEnum;

public class TestDownLoadETLEnum {

	@Test
	public void testDownLoadETLEnumIHIndex() {
		System.out.println(DownLoadFormatEnum.IH.ordinal());
	}
	@Test
	public void testDownLoadETLEnumTIMESTAMPIndex() {
		System.out.println(DownLoadFormatEnum.TIMESTAMP.ordinal());
	}
	@Test
	public void testDownLoadETLEnumMACCODEIndex() {
		System.out.println(DownLoadFormatEnum.MACCLEAN.ordinal());
	}
	
	@Test
	public void testCHANNELIDIndex() {
		//fail("Not yet implemented");
		System.out.println(DownLoadFormatEnum.CHANNEL_ID.ordinal());
	}
	@Test
	public void testCITY_IDIndex() {
		//fail("Not yet implemented");
		System.out.println(DownLoadFormatEnum.CITY_ID.ordinal());
	}
	@Test
	public void testCLIndex() {
		//fail("Not yet implemented");
		System.out.println(DownLoadFormatEnum.CL.ordinal());
	}
	@Test
	public void testIHIndex() {
		//fail("Not yet implemented");
		System.out.println(DownLoadFormatEnum.IH.ordinal());
	}
}
