/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: KeyCombinedDimensionUtil.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-9 下午4:05:21 
 * @input:输入日志路径/2013-9-9
 * @output:输出日志路径/2013-9-9
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bi.common.constant.DimensionConstant;

/**
 * @ClassName: KeyCombinedDimensionUtil
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-9 下午4:05:21
 */
public class KeyCombinedDimensionUtil {

    /**
     * 
     * 
     * @Title: getOutputKey
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param fields
     * @param @param groupByColumns
     * @param @return 参数说明
     * @return List<String> 返回类型说明
     * @throws
     */
    public static List<String> getOutputKey(String[] fields,
            int[] groupByColumns) {

        List<String> list = new ArrayList<String>(groupByColumns.length);
        int length = groupByColumns.length;
        int max = 1 << length - 1;
        for (int i = 0; i < max; i++) {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();
            for (int k = 1; k < length; k++) {
                map.put(new Integer(groupByColumns[k]), new Integer(
                        fields[groupByColumns[k]]));
            }
            for (int j = 0; j < length - 1; j++) {
                if ((i & (1 << j)) != 0) {
                    map.put(new Integer(groupByColumns[j + 1]), new Integer(
                            "-999"));
                }
            }

            Iterator<Integer> iterator = map.values().iterator();
            if (!iterator.hasNext()) {
                return list;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(fields[0] + "\t");
            for (;;) {
                Integer value = iterator.next();
                sb.append(value);
                if (iterator.hasNext()) {
                    sb.append("\t");

                }
                else {
                    break;
                }

            }
            list.add(sb.toString());
        }

        return list;

    }

}
