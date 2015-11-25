/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MapReduceConfInfoEnum.java 
 * @Package com.bi.video.calculate 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-10-21 下午2:46:34 
 * @input:输入日志路径/2013-10-21
 * @output:输出日志路径/2013-10-21
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.web.seo;

/**
 * @ClassName: MapReduceConfInfoEnum
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-10-21 下午2:46:34
 */
public enum MapReduceConfInfoEnum {
    inputPath("input"), outPutPath("output"), jobName("jobName"), reduceNum(
            "reduceNum"), isInputFormatLZOCompress("inpulzo"),isOutputFormatLZOCompress("oupulzo"),dateId("dateid");

    private MapReduceConfInfoEnum(String valueStr) {
        this.valueStr = valueStr;
    }

    private String valueStr;

    public String getValueStr() {
        return valueStr;
    }
}
