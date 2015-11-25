/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterLastDateByMACMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-21 下午2:26:03 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;

/**
 * @ClassName: FsPlayAfterLastDateByMACMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-21 下午2:26:03
 */
public class FsPlayAfterLastDateByMACMR {

    public static class FsPlayAfterLastDateByMACMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private String delim = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.delim = context.getConfiguration().get("delim");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String[] field = value.toString().trim().split(this.delim);
            if (field.length >= (FsplayAfterEnum.MAC.ordinal() + 1)) {
                String macString = MACFormatUtil
                        .macFormatToCorrectStr(field[FsplayAfterEnum.MAC
                                .ordinal()]);
                String dataIdStr = TimestampFormatUtil.formatTimestamp(
                        field[FsplayAfterEnum.TIMESTAMP.ordinal()]).get(
                        ConstantEnum.DATE_ID);
                context.write(new Text(macString), new Text(dataIdStr));

            }

        }

    }

    public static class FsPlayAfterLastDateByMACReduce extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeSet<String> dateTree = new TreeSet<String>();
            for (Text value : values) {
                dateTree.add(value.toString());

            }
            context.write(key, new Text(dateTree.last()));
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
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        String delim = ",";
        String lastDateByMACInputPaths = "input_playafter_1,input_playafter_2";
        Job job = new Job();
        job.setJarByClass(FsplayAfterCountMR.class);
        job.setJobName("FsPlayAfterLastDateByMACMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("delim", delim);
        FileInputFormat.setInputPaths(job, lastDateByMACInputPaths);
        FileOutputFormat.setOutputPath(job, new Path(
                "output_playafter_last_date_mac"));
        job.setMapperClass(FsPlayAfterLastDateByMACMapper.class);
        job.setReducerClass(FsPlayAfterLastDateByMACReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
