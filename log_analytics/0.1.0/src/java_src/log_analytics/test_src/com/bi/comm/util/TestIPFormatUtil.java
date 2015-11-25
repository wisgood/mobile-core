package com.bi.comm.util;

import static org.junit.Assert.*;

import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIPFormatUtil {
    long ipLong = 0l;
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIp2long() {
	
			ipLong= IPFormatUtil.ip2long("222.240.49.206");
			System.out.println(ipLong);
		
	}

	@Test
	public void testLong2ip() {
		
			System.out.println("A".toLowerCase());
		
	}
	
	@Test
	public void testVersionSub(){
	    
	    String versionInfo = "222.240.49.206";
	    String[] versionInfos = versionInfo.split("\\.");
	    for(String version: versionInfos){
	     System.out.println(version);   
	    }
	    
	    
	}

}
