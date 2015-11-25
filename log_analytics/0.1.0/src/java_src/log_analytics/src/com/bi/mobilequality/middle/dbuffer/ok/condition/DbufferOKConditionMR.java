/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DbufferOKCondition.java 
 * @Package com.bi.mobilequality.middle.dbuffer.ok.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-13 下午1:42:42 
 */
package com.bi.mobilequality.middle.dbuffer.ok.condition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


import com.bi.mobilequality.dbuffer.format.dataenum.DbufferQualityFormatEnum;

/**
 * @ClassName: DbufferOKCondition
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-13 下午1:42:42
 */
public class DbufferOKConditionMR {

    public static class DbufferOKConditionMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static Logger logger = Logger
                .getLogger(DbufferOKConditionMapper.class.getName());

        private String okStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.okStr = context.getConfiguration().get("ok");

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String orgiDataStr = value.toString();
                String[] splitSts = orgiDataStr.split("\t");
                String okTypeStr = splitSts[DbufferQualityFormatEnum.OK
                        .ordinal()];
                if (!(this.okStr.contains("-"))) {
                    if (okTypeStr.equalsIgnoreCase(this.okStr)) {
                        context.write(
                                new Text(
                                        splitSts[DbufferQualityFormatEnum.TIMESTAMP
                                                .ordinal()]), new Text(
                                        orgiDataStr));
                    }
                }
                else {
                    if (!(okTypeStr.equalsIgnoreCase(this.okStr.substring(1)))) {
                        context.write(
                                new Text(
                                        splitSts[DbufferQualityFormatEnum.TIMESTAMP
                                                .ordinal()]), new Text(
                                        orgiDataStr));
                    }

                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class DbufferOKConditionReduce extends
            Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, NullWritable.get());

            }

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
        String okStr = "-0";
        Job job = new Job();
        job.setJarByClass(DbufferOKConditionMR.class);
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("ok", okStr);
        FileInputFormat.addInputPath(job, new Path("output_dbuffer_quality"));
        FileOutputFormat.setOutputPath(job, new Path("output_dbuffer_quality_nook"));
        job.setMapperClass(DbufferOKConditionMapper.class);
        job.setReducerClass(DbufferOKConditionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
