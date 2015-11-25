package com.bi.common.etl.transform;

import java.io.File;
import java.util.Map;

import com.bi.common.etl.constant.ConstantEnum;
import com.bi.common.etl.pojo.AbstractDMDAO;
import com.bi.common.etl.pojo.DMURLRuleDAOImpl;

public class URL2IDTransform implements Transform {
    private AbstractDMDAO<String, Map<ConstantEnum, String>> dmURLRuleDAO = null;

    public URL2IDTransform() {
        try {
            dmURLRuleDAO = new DMURLRuleDAOImpl<String, Map<ConstantEnum, String>>();
            dmURLRuleDAO.parseDMObj(new File(ConstantEnum.DM_COMMON_URL.name()
                    .toLowerCase()));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String process(String origin, String sperator) {
        String result = null;
        Map<ConstantEnum, String> urlRuleMap = null;
        String urlFristID = "-1";
        String urlSecondID = "-1";
        String urlThirdID = "1";
        try {
            urlRuleMap = dmURLRuleDAO
                    .getDMOjb(origin);
            urlFristID = urlRuleMap.get(ConstantEnum.URL_FIRST_ID.name());
            urlSecondID = urlRuleMap.get(ConstantEnum.URL_SECOND_ID.name());
            urlThirdID = urlRuleMap.get(ConstantEnum.URL_THIRD_ID.name());
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        result = urlFristID + sperator + urlSecondID + sperator + urlThirdID;
        
        return result;
    }

}
