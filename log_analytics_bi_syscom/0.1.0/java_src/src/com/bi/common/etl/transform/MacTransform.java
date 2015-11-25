package com.bi.common.etl.transform;

public class MacTransform implements Transform {
    
    private boolean isUpper = true;
    
    public MacTransform(){
//        System.out.println("MacTransform---------------------------------------");
    }
    
    public void setIsUpper(String isUpper){
        if("true".equalsIgnoreCase(isUpper)){
            this.isUpper = true;
        }else if("false".equalsIgnoreCase(isUpper)){
            this.isUpper = false;
        }
    }

    @Override
    public String process(String origin, String sperator) {
        String result = "000000000000";
        if (origin == null||origin.equalsIgnoreCase("")) {
            return result;
        }
        if(isUpper){
             result = origin.replace(":", "").toUpperCase();
        }else{
            result = origin.replace(":", "").toLowerCase();
        }
        return result;
    }

}
