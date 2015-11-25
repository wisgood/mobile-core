/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercenTilesUtil.java 
 * @Package com.bi.common.util 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-26 上午11:27:10 
 * @input:输入日志路径/2013-8-26
 * @output:输出日志路径/2013-8-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @ClassName: PercenTilesUtil
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-26 上午11:27:10
 */
public class PercenTilesUtil {

    /**
     * 
     * 
     * @Title: percenTitle
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param p
     * @param @param longArrgs
     * @param @return 参数说明
     * @return double 返回类型说明
     * @throws
     */
    public static double percenTitle(float p, long[] longArrgs) {
        int arraysLength = longArrgs.length;
        if (arraysLength > 1) {
            // 取k值
            int k = (int) (arraysLength * p);
            int argIndex = k - 1;
            if (argIndex < 0) {
                argIndex = 0;
            }
            // 数据进行排序
            Arrays.sort(longArrgs);
            if (argIndex == arraysLength - 1) {
                return longArrgs[argIndex];
            }
            // 计算百分位数
            double percenTitleFloat = (arraysLength * p - k)
                    * (longArrgs[argIndex + 1] - longArrgs[argIndex])
                    + longArrgs[argIndex];
            if ((long) percenTitleFloat > longArrgs[arraysLength - 1]) {
                return longArrgs[arraysLength - 1];
            }
            return percenTitleFloat;
        }
        else {
            return longArrgs[0];
        }

    }

    public static List<Long> calculatePn(TreeMap<Long, Long> treemap, int count) {

        List<Long> pnList = new ArrayList<Long>(20);
        // 累计频次
        long eflsum = 0;
        TreeMap<Long, Long> eflMap = new TreeMap<Long, Long>();
        Iterator<Entry<Long, Long>> iter = treemap.entrySet().iterator();
        Map<Integer, Long> percentEflMap = new TreeMap<Integer, Long>();
        List<Integer> percentList = new ArrayList<Integer>(100);
        while (iter.hasNext()) {
            Entry<Long, Long> entry = iter.next();
            eflsum += entry.getValue().longValue();
            eflMap.put(eflsum, entry.getKey().longValue());
            BigDecimal bg = new BigDecimal((double) eflsum / count);
            int percentValue = (int) (bg.setScale(2, BigDecimal.ROUND_HALF_UP)
                    .doubleValue() * 100);
            percentEflMap.put(percentValue, eflsum);
            percentList.add(percentValue);
        }

        for (int i = 5; i < 100; i = i + 5) {
            float percent = (float) i / 100;
            // long indexValue = (long) (percent * count);
            long[] efvalues = getEflValues(i, percentList, percentEflMap);
            long efl = efvalues[0];
            long ii = efvalues[1];
            long lm = eflMap.get(efl);
            long fm = treemap.get(lm);
            System.out.println("percent:" + percent);
            System.out.println("lm:" + lm);
            System.out.println("fm:" + fm);
            System.out.println("efl:" + efl);
            System.out.println("ii:" + ii);
            System.out.println("n:" + efl);
            System.out.println("percenTitle:"
                    + percenTitle(lm, fm, ii, efl, count, percent));

            pnList.add(percenTitle(lm, fm, ii, efl, count, percent));
        }
        pnList.add(treemap.lastKey());
        return pnList;
    }

    public static long percenTitle(long lm, long fm, long ii, long efl, int n,
            float percent) {
        return (long) (lm + ii / fm * (n * percent - efl));
    }

    private static long[] getEflValues(int indexValue,
            List<Integer> percentList, Map<Integer, Long> percentEflMap) {
        long[] values = { 0, 0 };
        for (int i = 0; i < percentList.size(); i++) {
            int percentValue = percentList.get(i);
            if (indexValue < percentValue) {
                int beforeFecValue = 0;
                if (i > 0) {
                    beforeFecValue = percentList.get(i - 1);
                }
                System.out.println(percentEflMap.get(beforeFecValue));
                System.out.println(percentEflMap.get(percentValue));
                values[1] = (long) Math.abs(percentEflMap.get(beforeFecValue)
                        - percentEflMap.get(percentValue));
                values[0] = percentEflMap
                        .get(Math.abs(indexValue - beforeFecValue) < Math
                                .abs(indexValue - percentValue) ? percentValue
                                : beforeFecValue);
                return values;
            }

        }
        return values;
    }

    public static List<Long> init() {
        long[] inits = { 5009, 5831, 5538, 6484, 5582, 14094, 14969, 6783,
                17106, 5832, 5537, 9329, 7248, 7867, 8203, 14880, 5775, 12223,
                13279, 5653, 36923, 1410112, 48663, 5910, 2147483647,
                2147483647, 6386, 3251936, 2147483647, 13897091, 219232671,
                3973522, 49830867, 53458, 66564681, 29369253, 592318079,
                4161507, 218293661, 3323833 };
        // long[] inits ={0};

        // Random r = new Random();
        List<Long> dateList = new ArrayList<Long>(inits.length);

        for (int i = 0; i < inits.length; i++) {

            dateList.add(inits[i]);

        }

        return dateList;

    }
}
