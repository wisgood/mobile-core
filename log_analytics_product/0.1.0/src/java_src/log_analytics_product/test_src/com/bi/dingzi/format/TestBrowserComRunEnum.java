package com.bi.dingzi.format;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBrowserComRunEnum {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSUCindex() {
        System.out.println(BrowserComRunEnum.SUC.ordinal()) ;
    }
    @Test
    public void testTimeCindex() {
        System.out.println(BrowserComRunEnum.TIME.ordinal()) ;
    }
    @Test
    public void testMACCindex() {
        System.out.println(BrowserComRunEnum.MAC.ordinal()) ;
    }
    @Test
    public void testURL(){
        
        String url = "http://fs.funshion.com/client_play_after_2/006B8984AAEBC15BEFABBA5E0374A2ED19B1D6AE/adback?c=dy,kb,null&ver=2.8.5.24,1";
           url.indexOf("://");
      
        System.out.println( url.substring(url.indexOf("://")+3));
        
    }

}
