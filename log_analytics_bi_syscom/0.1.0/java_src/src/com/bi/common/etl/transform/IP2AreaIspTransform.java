package com.bi.common.etl.transform;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.bi.common.etl.constant.ConstantEnum;
import com.bi.common.etl.pojo.AbstractDMDAO;
import com.bi.common.etl.pojo.DMIPRuleDAOImpl;

public class IP2AreaIspTransform implements Transform {

    private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;
    public enum OutputFormat{PROVINCE, PROVINCE_ISP, CITY_ISP, PROVINCE_CITY_ISP};
    private OutputFormat defaultFormat = OutputFormat.PROVINCE_CITY_ISP;

    public IP2AreaIspTransform() {
        dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
        try {
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    public void setOutputFormat(String outputFormat){
        if("province".equalsIgnoreCase(outputFormat)){
            defaultFormat = OutputFormat.PROVINCE;
        }
        if("province_isp".equalsIgnoreCase(outputFormat)){
            defaultFormat = OutputFormat.PROVINCE_ISP;
        }
        if("city_isp".equalsIgnoreCase(outputFormat)){
            defaultFormat = OutputFormat.CITY_ISP;
        }
        if("province_city_isp".equalsIgnoreCase(outputFormat)){
            defaultFormat = OutputFormat.PROVINCE_CITY_ISP;
        }
    }

    @Override
    public String process(String origin, String sperator) {

        long ip = Long.parseLong(origin);
        Map<ConstantEnum, String> ipRuleMap;
        String provenceId, cityId, ispId;
        String result = null;
        try {
            ipRuleMap = dmIPRuleDAO.getDMOjb(ip);
            provenceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
        }
        catch(Exception e) {
            provenceId = "-1";
            cityId = "-1";
            ispId = "-1";
            e.printStackTrace();
        }
        
        if (defaultFormat.ordinal() == OutputFormat.PROVINCE.ordinal()) {
            result = provenceId;
            result += sperator + cityId;
            result += sperator + ispId;
        }   
        if (defaultFormat.ordinal() == OutputFormat.PROVINCE_ISP.ordinal()) {
            result = provenceId;
            result += sperator + ispId;
        }
        if (defaultFormat.ordinal() == OutputFormat.CITY_ISP.ordinal()) {
            result = cityId;
            result += sperator + ispId;
        }
        if (defaultFormat.ordinal() == OutputFormat.PROVINCE_CITY_ISP.ordinal()) {
            result = provenceId;
            result += sperator + cityId;
            result += sperator + ispId;
        }
        
        return result;
    }

}
