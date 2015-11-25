/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: LandingPageMR.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-23 下午2:30:57 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.comm.util.StringDecodeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;

/**
 * @ClassName: LandingPageMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-23 下午2:30:57
 */
public class LandingPageMR {

    public static class LandingPageMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orginData = value.toString();
            String[] fields = orginData.split("\t");
            if (fields.length > PVNewEnum.URL.ordinal()) {

                String urlStr = StringDecodeFormatUtil.decodeCodedStr(
                        fields[PVNewEnum.URL.ordinal()].trim(), "UTF-8");
                String timestampStr = fields[PVNewEnum.TIMESTAMP.ordinal()]
                        .trim();
                String sessionidStr = fields[PVNewEnum.SESSIONID.ordinal()]
                        .trim();
                if (!"".equalsIgnoreCase(urlStr)) {
                    context.write(new Text(sessionidStr), new Text(timestampStr
                            + "\t" + urlStr));
                }
            }
        }

    }

    public static class LandingPageReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Long, String> timeUrlsTree = new TreeMap<Long, String>();
            for (Text value : values) {

                String valueStr = value.toString();
                String[] splists = valueStr.split("\t");
                Long timestamp = new Long(splists[0]);
                timeUrlsTree.put(timestamp, splists[1]);

            }

            Long timeStamp = timeUrlsTree.firstKey();
            String urlStr = timeUrlsTree.get(timeStamp);
            context.write(
                    key,
                    new Text(TimestampFormatUtil
                            .formatTimeStamp(timeStamp + "") + "\t" + urlStr));
        }

    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        String inputPaths = "input_new_pv";
        Job job = new Job();
        job.setJarByClass(PVfcktimesidCaMR.class);
        job.setJobName("LandingPageMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat
                .setOutputPath(job, new Path("output_new_sid_time_url"));
        job.setMapperClass(LandingPageMapper.class);
        job.setReducerClass(LandingPageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
