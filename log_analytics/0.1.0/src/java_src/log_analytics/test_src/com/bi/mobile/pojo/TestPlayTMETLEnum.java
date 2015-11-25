package com.bi.mobile.pojo;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.playtm.format.dataenum.PlayTMEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMFormatEnum;

public class TestPlayTMETLEnum {

    @Test
    public void testIHIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.IH.ordinal());
    }

    @Test
    public void testIsp_idIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.ISP_ID.ordinal());
    }

    @Test
    public void testVTMIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.VTM.ordinal());
    }

    @Test
    public void testCHANNELIDIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.CHANNEL_ID.ordinal());
    }

    @Test
    public void testTIMESTAMPIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.TIMESTAMP.ordinal());
    }

    @Test
    public void testCITY_IDIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.CITY_ID.ordinal());
    }

    @Test
    public void testRTIndex() {
        // fail("Not yet implemented");
        System.out.println(PlayTMFormatEnum.RT.ordinal());
    }

    @Test
	public void testTowPoint(){
	    
	    String testStr = "1368545688,118.26.250.222,iPhone_6.1_iPhone3.1,6C:C2:6B:46:A3:4C,1,ef0d23f9b187bff62fd4f9f3197b59c1,0.000000,1099000.000000,993295.375000,2,747.703003,1.2.0.2,1022";
	    String []splitSts =  testStr.split(",");
	   
	    try {
            long vtmlong = FormatMobileUtil.parseDoubleToLong(splitSts,
                    PlayTMEnum.class.getName(),
                    PlayTMEnum.VTM.toString());
            long pnlong = FormatMobileUtil.parseDoubleToLong(splitSts,
                    PlayTMEnum.class.getName(),
                    PlayTMEnum.PN.toString());
            long tulong = FormatMobileUtil.parseDoubleToLong(splitSts,
                    PlayTMEnum.class.getName(),
                    PlayTMEnum.TU.toString());
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	  
	    
	}
    
    @Test
    public void testNan(){
        String testStr ="1368545360,221.126.13.3,iPhone_5.0.1_iPhone3.1,50:EA:D6:AC:F3:DC,1.1.6.2,1,(null),0.000000,0.000000,nan,1015,0,0.000000";
        String []testArgs = testStr.split(",");
        System.out.println();
        
    }
    
    @Test
    public void testReversion(){
        String testStr = "1368546395,110.191.33.60,iPhone_5.1.1_iPhone4.1,D0:23:DB:95:8A:7F,2,48f1c542e25e909294cc17911a7bd3c9,0.000000,2610000.000000,2642895.500000,1,13.790965,1.2.0.2,1022";
         String[] testArgs = testStr.split(",");
        System.out.println(testStr.split(","));
    }
}
