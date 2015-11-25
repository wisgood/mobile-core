package com.bi.common.etl.transform;

public class IP2LongTransform implements Transform {
    
    public IP2LongTransform(){
//        System.out.println("IP2LongTransform---------------------------------------");
    }

    @Override
    public String process(String origin, String sperator) {
        String result = "0";
        if(origin == null || origin.isEmpty()){
            return result;
        }
        String[] ips = origin.split("\\.");
//        System.out.println("111" + origin);
        Long ipLong = 0l;
        try {
            for (String ip : ips) {
                ipLong = ipLong << 8;
                ipLong += Long.parseLong(ip);
            }
        }
        catch(Exception e) {
//            e.printStackTrace();
            return result;
        }
        result = ipLong.toString();
        return result;
    }

}
