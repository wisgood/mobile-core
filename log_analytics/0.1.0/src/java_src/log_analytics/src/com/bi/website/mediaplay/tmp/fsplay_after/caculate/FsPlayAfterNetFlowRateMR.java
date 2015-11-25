/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterNetFlowRateMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-29 下午4:06:11 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import com.bi.comm.util.StringUtils;

/**
 * @ClassName: FsPlayAfterNetFlowRateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-29 下午4:06:11
 */
public class FsPlayAfterNetFlowRateMR {

    public static class FsPlayAfterNetFlowRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // super.map(key, value, context);
            String orgdataStr = value.toString().trim();
            String[] fields = orgdataStr.split("\t");
            StringBuilder dataSB = new StringBuilder();
            if (fields.length == 5) {
                // dataSB.append(fields[0]);
                // dataSB.append("\t");
                long playtmlong = Long.parseLong(fields[1]);
                dataSB.append(playtmlong);
                dataSB.append("\t");
                dataSB.append(fields[2]);
                dataSB.append("\t");
                Date startDate = StringUtils.stringToDate("20130430",
                        "yyyyMMdd");
                Date endDate = StringUtils.stringToDate(fields[3], "yyyyMMdd");
                long margin = startDate.getTime() - endDate.getTime();
                margin = margin / (1000 * 60 * 60 * 24);
                dataSB.append(margin);
                dataSB.append("\t");
                long networkflowlong = Long.parseLong(fields[4]);
                dataSB.append(fields[4]);
                dataSB.append("\t");
                dataSB.append(networkflowlong / playtmlong);
                context.write(new Text(fields[0]), new Text(dataSB.toString()));
            }

        }

    }

    public static class FsPlayAfterNetFlowRateReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Text value : values) {
                context.write(key, value);

            }
        }

    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
