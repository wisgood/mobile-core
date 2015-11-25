package com.bi.comm.util;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TreeSet;

import org.junit.Test;

public class TestTimestampFormatUtil {

	@Test
	public void testFormatTimestamp() {
	  
	    System.out.println(TimestampFormatUtil.formatTimeStamp("1369209223"));
		//System.out.println(TimestampFormatUtil.formatTimestamp("1363708821"));
		
	}

	@Test
	public void testTimestampToStr(){
		
		Timestamp ts = new Timestamp(System.currentTimeMillis());  
		System.out.println(ts);
	}
	
	@Test
	public void testGet24HourMill(){
		
		 SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		 System.out.println(time.format(TimestampFormatUtil.get24HourMill()));
	}
	@Test
	public void testABC(){
		
		
		String ab = "1363708807,120.14.85.106,apad_4.0.4_M700,48:02:2a:7b:01:a3,1.5.2.4,1,38bb2b4bd3a5f8965663552303aa530f,121.18.237.35,0,0,3549,-1,1037,256,0";
		System.out.println(ab.split(",").length);
	}
	@Test
	public void testDtoL(){
		
		String abc ="2354107.284816";
		double vtmDouble = Double.parseDouble(abc);
		long vtmlong = (long)vtmDouble;
		System.out.println(vtmlong);
		
	}
	
	@Test
	public void testArrayListSort(){
	    
	    TreeSet<String> tnp = new TreeSet<String>();
	    tnp.add("2013");
	    tnp.add("2012");
	    System.out.println(tnp.last());
	}
	
	@Test
	public void subStringTest(){
	    String v ="sumcount";
	    String a =v +"0";
	    System.out.println(a.substring(v.length()));
	}
	
}
