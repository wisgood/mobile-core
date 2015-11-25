package com.bi.mobile.pojo;

import org.junit.Test;

import com.bi.mobile.bootstrap.format.dataenum.BootStrapEnum;
import com.bi.mobile.fbuffer.format.dataenum.FbufferFormatEnum;
import com.bi.mobilequality.fbuffer.format.dataenum.FbufferEnum;


public class TestFbufferETLEnum {

	@Test
	public void testIHIndex() {
		System.out.println(FbufferFormatEnum.IH.ordinal());
	}

	@Test
	public void testFbufferETLEnumTIMESTAMPIndex() {
		System.out.println(FbufferFormatEnum.TIMESTAMP.ordinal());
	}
	@Test
	public void testFbufferETLEnumMACCODEIndex() {
		System.out.println(FbufferFormatEnum.MACCLEAN.ordinal());
	}
	@Test
	public void testFbufferETLEnumCHANNEL_IDIndex() {
		System.out.println(FbufferFormatEnum.CHANNEL_ID.ordinal());
	}
	
	@Test
	public void testFbufferETLEnumMEDIA_IDIndex() {
		System.out.println(FbufferFormatEnum.MEDIA_ID.ordinal());
	}
	@Test
	public void testFbufferETLEnumCITY_IDIndex() {
		System.out.println(FbufferFormatEnum.CITY_ID.ordinal());
	}
	   @Test
	    public void testFbufferETLEnumOKIndex() {
	        System.out.println(FbufferEnum.OK.ordinal());
	    }
	
	@Test
	public void test(){
		
		System.out.println("1363708809,220.168.129.121,iPhone_5.1.1_iPhone4.1,74:E2:F5:4E:64:77,1.2.1.1,1,a298722a5a1ae797b38429647b2cc9b0,182.118.38.71,0,0.000000,38628.015625,-1.000000,0,1363708809,10.195.158.106".split(",").length);
	}
	
	@Test
	public void testServerIP(){
		
		try {
			Class<Enum> logEnum = (Class<Enum>) Class.forName(BootStrapEnum.class.getName());
			Enum.valueOf(logEnum, "SERVERIP").ordinal();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    	
	}
}
