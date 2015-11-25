/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlatformBootJCMR.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-14 上午11:13:19 
 */
package com.bi.dingzi.day.user;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.init.CommArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.dingzi.format.FsPlatformBootEnum;

/**
 * @ClassName: FsPlatformBootJCMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-14 上午11:13:19
 */
public class FsPlatformBootJCMR extends Configured implements Tool {

    public static class FsPlatformBootJCMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split("\t");
            String timeStampStr = fields[FsPlatformBootEnum.TIME.ordinal()];
            String versionInfo = fields[FsPlatformBootEnum.VERSION.ordinal()];
            String nameStr = fields[FsPlatformBootEnum.NAME.ordinal()];
            String bootmethodStr = fields[FsPlatformBootEnum.BOOTMETHOD
                    .ordinal()];
            try {
                StringBuilder valueSB = new StringBuilder();
                // 日期id
                String dateIdStr = TimestampFormatUtil.formatTimestamp(
                        timeStampStr).get(ConstantEnum.DATE_ID);
                // MAC地址
                String macStr = MACFormatUtil
                        .macFormatToCorrectStr(fields[FsPlatformBootEnum.MAC
                                .ordinal()]);
                String clientstateStr = fields[FsPlatformBootEnum.CLIENTSTATE
                        .ordinal()];
                // versionId
                long versionId = -0l;
                versionId = IPFormatUtil.ip2long(versionInfo);
                valueSB.append(versionId);
                valueSB.append("\t");
                // nameID
                int nameid = Math.abs(nameStr.hashCode());
                valueSB.append(nameid);
                int bootmethodID = Integer.parseInt(bootmethodStr);
                valueSB.append("\t");
                valueSB.append(bootmethodID);
                valueSB.append("\t");
                valueSB.append(macStr);
                valueSB.append("\t");
                valueSB.append(clientstateStr);
                context.write(new Text(dateIdStr), new Text(valueSB.toString()));
            }
            catch(NumberFormatException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class FsPlatformBootJCReduce extends
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
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        CommArgs commArgs = new CommArgs();
        commArgs.init("fsplatformbootjcmr.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new FsPlatformBootJCMR(),
                commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FsPlatformBootJCMR");
        job.setJarByClass(FsPlatformBootJCMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsPlatformBootJCMapper.class);
        job.setReducerClass(FsPlatformBootJCReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
