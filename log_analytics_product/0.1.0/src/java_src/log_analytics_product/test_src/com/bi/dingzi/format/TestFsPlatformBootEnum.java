package com.bi.dingzi.format;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFsPlatformBootEnum {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    @Test
    public void testTIMEIndex() {
      System.out.println(FsPlatformBootEnum.TIME.ordinal());
    }

    
    @Test
    public void testMACIndex() {
      System.out.println(FsPlatformBootEnum.MAC.ordinal());
    }

    
    @Test
    public void testDouble2() throws ParseException{
        double f =3;
        double a =2;
        BigDecimal bg = new BigDecimal(a/f);
        double f1 = bg.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println(f1);
        String fs = "20130530";
        
        SimpleDateFormat df=new SimpleDateFormat("yyyyMMdd"); 
        Date d=df.parse(fs);
        System.out.println("今天的日期："+df.format(d));   
        System.out.println("两天前的日期：" + df.format(new Date(d.getTime() - 5 * 24 * 60 * 60 * 1000)));  
        System.out.println("三天后的日期：" + df.format(new Date(d.getTime() + 3 * 24 * 60 * 60 * 1000)));
    }
    
    @Test
    public void testStpi(){
        
        String abc ="20or21or22or23";
        String []splits = abc.split("or");
        for(int i=0;i<splits.length;i++){
            
            System.out.println(splits[i]);
        }
    }
    
    @Test
    public void testHashMap(){
        
        Map<Long,String> testMap = new HashMap<Long,String>();
        testMap.put(new Long(100000), "100000");
        testMap.put(new Long(100000), "300000");
        testMap.put(new Long(100000), "400000");
        
        System.out.println(testMap.get(new Long(100000)));
        
        System.out.println(testMap.size());
        
        
    }
    
}
