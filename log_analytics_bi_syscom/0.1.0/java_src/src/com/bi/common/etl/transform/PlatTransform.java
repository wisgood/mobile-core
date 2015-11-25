package com.bi.common.etl.transform;


public class PlatTransform implements Transform {

    public PlatTransform(){
//        System.out.println("PlatTransform---------------------------");
    }
    
    private enum Plat {
        PC_CLIENT("client", "1"), PC_WEB("web", "2"), MOBILE_IPHONE("iphone", "3"), 
        MOBILE_IPAD("ipad", "4"), MOBILE_APAD("apad", "5"), MOBILE_APHONE("aphone", "6"),
        MOBILE_WPAD("wpad", "7"), MOBILE_WPHONE("winphone", "8"), FLASH("flash", "10"),
        MOBILE_OTHER("other", "9");

        private String name;

        private String index;
        


        private Plat(String name, String index) {
            this.name = name;
            this.index = index;
        }
        
        public void setName(String name){
            this.name = name;
        }
        
        public void setIndex(String index){
            this.index = index;
        }

    };

    @Override
    public String process(String origin, String sperator) {
        String result = Plat.MOBILE_OTHER.index;
        String originLower = origin.toLowerCase();
        for (Plat p : Plat.values()) {
            if (originLower.contains(p.name)) {
                result = p.index;
//                break;
            }
        }
        return result;
    }
}
