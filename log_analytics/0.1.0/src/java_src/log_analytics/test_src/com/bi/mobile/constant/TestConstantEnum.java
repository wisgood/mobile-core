package com.bi.mobile.constant;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bi.mobile.bootstrap.format.dataenum.BootStrapFormatEnum;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.fbuffer.format.dataenum.FbufferFormatEnum;
import com.bi.mobile.fbuffer.format.dataenum.FbufferEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMFormatEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMEnum;

public class TestConstantEnum {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConstantEnum() {
		System.out.println(ConstantEnum.MAC);
	}
	@Test
	public void testPojoEnum(){
		String playStr ="1363746898,61.55.156.20,aphone_4.0.3_HTC+T328w,1C:B0:94:EE:0A:AD,1.5.2.4,2,83a92d5930a58a4aea96abcb18cac3fa,0,671067,1553433,1010,0,0,1,Mac=1C:B0:94:EE:0A:AD";
		System.out.println(playStr.split(",")[PlayTMEnum.VTM.ordinal()]);
		String playEtlStr="1363746898	61.55.156.20	aphone_4.0.3_HTC+T328w	1C:B0:94:EE:0A:AD	1.5.2.4	2	83a92d5930a58a4aea96abcb18cac3fa	0	671067	1553433	1010	0	0	1	Mac=1C:B0:94:EE:0A:AD	20130319	19	3	17105412	1010	-1	7	31544738450093	-1	-1	13	5";
	    System.out.println(playEtlStr.split("\t")[PlayTMFormatEnum.VTM.ordinal()]);
	
	    String sta = "1.02356";
	    double ab =Double.parseDouble(sta);
	    System.out.println((long)ab);
       
	    String keyStr = "DATE_ID";
	    
	    ConstantEnum constantEnum = Enum.valueOf(ConstantEnum.class, keyStr);
	    System.out.println(constantEnum.name());
	    System.out.println(constantEnum.ordinal());
	    
	    try {
			Class.forName(BootStrapFormatEnum.class.getName());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    String str = "1363708801	113.107.218.34	iPad_6.1.2_iPad2.1	28:6A:BA:76:1F:90	1.2.2.1	1	(null)	(null)	-3	0.000000	2589.115143	-1.000000	0	1363708801	20130319	9	2	16908801	0	-1	249	44438859947920	-1	-1	44	1	0	";
	    System.out.println(str.split("\t").length);
	    System.out.println(FbufferFormatEnum.SERVER_ID.ordinal());
	    
	    System.out.println( FbufferFormatEnum.values().length);
	
	}
}
