/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercenTiles.java 
 * @Package com.bi.comm.util 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-14 上午10:24:21 
 */
package com.bi.common.util;

import java.util.Arrays;

/**
 * @ClassName: PercenTiles
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-14 上午10:24:21
 */
public class PercenTilesUtil {

    public static double percenTitle(float p, long[] longArrgs) {

        if (longArrgs.length > 1) {
            // 取k值
            int k = (int) (longArrgs.length * p);
            
            int argIndex = k-1;
            // 数据进行排序
            Arrays.sort(longArrgs);
            // 计算百分位数
            double percenTitleFloat = (longArrgs.length * p - k)
                    * (longArrgs[argIndex + 1] - longArrgs[argIndex]) + longArrgs[argIndex];
            return percenTitleFloat;
        }
        else {

            return longArrgs[0];
        }

    }
}
