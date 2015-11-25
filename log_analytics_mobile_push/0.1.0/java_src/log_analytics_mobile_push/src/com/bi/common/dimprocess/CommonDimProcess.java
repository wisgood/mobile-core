package com.bi.common.dimprocess;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Description: 维度处理的类
 * @author wangxiaowei
 * @date 2013-7-25 下午10:50:51 
 * @input:以tab分割的键值对，前面是维度ID，后面是维度名称，每个维度占一行，以＃号开头的为注释信息
 * @output:
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class CommonDimProcess<E, T> extends AbstractDimProcess<E, T> {
    private static Logger logger = Logger.getLogger(CommonDimProcess.class
            .getName());

    private Map<String, String> dimMap = null;

    /**
     * (非 Javadoc)
     * <p>
     * Title: parseDimensionFile
     * </p>
     * <p>
     * Description:read dimension file ,input the dimention file path
     * </p>
     * 
     * @param file
     * @throws IOException
     * @see com.bi.dingzi.dimensionprocess.AbstractDimProcess#parseDimensionFile(java.io.File)
     */
    @Override
    public void parseDimensionFile(File file) throws IOException {
        BufferedReader in = null;
        try {
            dimMap = new HashMap<String, String>();
            in = new BufferedReader(new InputStreamReader(new FileInputStream(
                    file)));
            String line = null;
            while ((line = in.readLine()) != null) {
                if (line.contains("#")) {
                    continue;
                }
                String[] strPlate = line.split("\t");
                dimMap.put(strPlate[1].toLowerCase(), strPlate[0]);
            }
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
            logger.error("维度处理失败:" + e.getMessage(), e.getCause());
            throw (IOException) e;
        }

        finally {
            if (null != in)
                in.close();
        }

    }

    /**
     * (非 Javadoc)
     * <p>
     * Title: getDimensionId
     * </p>
     * <p>
     * Description:return the dimention id ,commonly the dimension id is a
     * integar
     * </p>
     * 
     * @param param
     * @return
     * @throws Exception
     * @see com.bi.dingzi.dimensionprocess.AbstractDimProcess#getDimensionId(java.lang.Object)
     */
    @Override
    public T getDimensionId(E param) {
        // TODO Auto-generated method stub
        String id = "-1";
        try {
            id = dimMap.get((String) param);
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
            e.printStackTrace();
        }
        return (T) id;
    }

}
