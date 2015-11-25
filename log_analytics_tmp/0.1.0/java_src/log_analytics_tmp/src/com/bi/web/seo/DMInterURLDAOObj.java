/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DMInterURLDAOObj.java 
 * @Package com.bi.web.seo 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-7 下午4:10:03 
 * @input:输入日志路径/2013-11-7
 * @output:输出日志路径/2013-11-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.web.seo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: DMInterURLDAOObj
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-7 下午4:10:03
 */
public class DMInterURLDAOObj {
    private Map<String, String> dmInUrlMap = null;

    public DMInterURLDAOObj(File file) throws IOException {
        // TODO Auto-generated method stub
        BufferedReader inFile = null;
        try {
            this.dmInUrlMap = new HashMap<String, String>();

            inFile = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file)));
            String line = null;
            while ((line = inFile.readLine()) != null) {
                String[] strURLLine = line.trim().split("\t");
                String secondId = strURLLine[DMInterURLEnum.SECOND_ID.ordinal()];
                String secondName = strURLLine[DMInterURLEnum.SECOND_NAME
                        .ordinal()];
                dmInUrlMap.put(secondId, secondName);
            }

        }
        catch(Exception e) {
            e.printStackTrace();
        } finally {
            inFile.close();
        }
    }

    public String getSecondNameBySecondId(String secondId) {

        return this.dmInUrlMap.get(secondId);
    }

}
