/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BouncerateByURLMR.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-24 上午9:55:03 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.comm.dm.pojo.dao.DMTopnURLDAO;
import com.bi.comm.util.StringDecodeFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;

/**
 * @ClassName: BouncerateByURLMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-24 上午9:55:03
 */
public class BouncerateByURLMR {

    public static class BouncerateByURMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private DMTopnURLDAO dmTopnURLDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.dmTopnURLDAO = new DMTopnURLDAO();
            if (this.isLocalRunMode(context)) {
                String dmTopnURLFilePath = context.getConfiguration().get(
                        ConstantEnum.TOPNURL_FILEPATH.name());
                this.dmTopnURLDAO.parseDMObj(new File(dmTopnURLFilePath));
            }
            else {
                File dmTopnURLFile = new File(ConstantEnum.TOPNURL.name()
                        .toLowerCase());
                this.dmTopnURLDAO.parseDMObj(dmTopnURLFile);
            }
        }

        private boolean isLocalRunMode(Context context) {
            String mapredJobTrackerMode = context.getConfiguration().get(
                    "mapred.job.tracker");
            if (null != mapredJobTrackerMode
                    && ConstantEnum.LOCAL.name().equalsIgnoreCase(
                            mapredJobTrackerMode)) {
                return true;
            }
            return false;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orgindataStr = value.toString();
            String[] splits = orgindataStr.split("\t");
            if (splits.length > PVNewEnum.URL.ordinal()) {

                String urlStr = StringDecodeFormatUtil.decodeCodedStr(
                        splits[PVNewEnum.URL.ordinal()].trim(), "UTF-8");
                String sessionidStr = splits[PVNewEnum.SESSIONID.ordinal()]
                        .trim();
                if (this.dmTopnURLDAO.isContainsURL(urlStr)) {
                    context.write(new Text(urlStr), new Text(sessionidStr));
                }

            }
        }
    }

    public static class BouncerateByURReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            Map<String, Integer> sessionMap = new HashMap<String, Integer>();
            for (Text value : values) {
                String sessionIdStr = value.toString();
                if (sessionMap.containsKey(sessionIdStr)) {
                    int count = sessionMap.get(sessionIdStr);
                    count++;
                    sessionMap.put(sessionIdStr, count);
                }
                else {
                    int count = 1;
                    sessionMap.put(sessionIdStr, count);
                }

            }
            double bounceRateCount = 0;
            double totalCount = 0;
            Iterator iter = sessionMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter
                        .next();
                if (entry.getValue() == 1) {
                    bounceRateCount++;
                }
                totalCount++;
            }
            double bounceRate = bounceRateCount / totalCount;
            context.write(key, new Text((long)bounceRateCount + "\t" + (long)totalCount
                    + "\t" + bounceRate));
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
        job.setJarByClass(BouncerateByURLMR.class);
        job.setJobName("BouncerateByURLMR");
        job.getConfiguration().set(ConstantEnum.TOPNURL_FILEPATH.name(),
                "conf/topnurl");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job,
                new Path("output_new_pv_bouncerate"));
        job.setMapperClass(BouncerateByURMapper.class);
        job.setReducerClass(BouncerateByURReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
