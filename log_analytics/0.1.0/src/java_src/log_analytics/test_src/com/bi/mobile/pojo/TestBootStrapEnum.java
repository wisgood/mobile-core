package com.bi.mobile.pojo;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bi.mobile.bootstrap.format.dataenum.BootStrapEnum;

public class TestBootStrapEnum {

	@Test
	public void testRTLenght() {
	 System.out.println(BootStrapEnum.RT.ordinal()+1);
	}

	@Test
	public void testBTypeIndex() {
	 System.out.println(BootStrapEnum.BTYPE.ordinal()+1);
	}
	
	@Test
    public void testOKIndex() {
     System.out.println(BootStrapEnum.OK.ordinal()+1);
    }
}

