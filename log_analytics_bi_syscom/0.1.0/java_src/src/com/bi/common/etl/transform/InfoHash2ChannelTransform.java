package com.bi.common.etl.transform;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bi.common.etl.constant.ConstantEnum;
import com.bi.common.etl.pojo.AbstractDMDAO;
import com.bi.common.etl.pojo.DMInfoHashRuleDAOImpl;


public class InfoHash2ChannelTransform implements Transform {

    private AbstractDMDAO<String, Map<ConstantEnum, String>>
    dmInfoHashRuleDAO = null;
    
    public InfoHash2ChannelTransform(){
        this.dmInfoHashRuleDAO = new DMInfoHashRuleDAOImpl<String, Map<ConstantEnum, String>>();
        
        try {
            dmInfoHashRuleDAO.parseDMObj(new File(ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name()
                    .toLowerCase()));
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public String process(String origin, String sperator) {
        Map<ConstantEnum,String> infoHashMap = new HashMap<ConstantEnum,String>();
        try {
            infoHashMap = dmInfoHashRuleDAO.getDMOjb(origin);
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
