/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByColMRUTL.java 
 * @Package com.bi.comm.calculate.percentiles 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-14 上午10:12:33 
 */
package com.bi.comm.calculate.percentiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.comm.util.PercenTilesUtil;

/**
 * @ClassName: PercentilesByColMRUTL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-14 上午10:12:33
 */
public class PercentilesByColMRUTL {

    public static class PercentilesByColMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {
        private String[] colNum = null;

        private String needCaPnColNum = null;

        @Override
        public void setup(Context context) {
            this.colNum = context.getConfiguration().get("groupby").split(",");
            this.needCaPnColNum = context.getConfiguration().get("pncolindex");
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder colKeySb = new StringBuilder();
            String[] field = value.toString().trim().split("\t");
            for (int i = 0; i < colNum.length; i++) {
                colKeySb.append(field[Integer.parseInt(colNum[i])]);
                if (i < colNum.length - 1) {
                    colKeySb.append("\t");
                }
            }
            long needCalPn = Long.parseLong(field[Integer
                    .parseInt(this.needCaPnColNum)]);
            context.write(new Text(colKeySb.toString()), new LongWritable(
                    needCalPn));
        }

    }

    public static class PercentilesByColReducer extends
            Reducer<Text, LongWritable, Text, Text> {
        private static Logger logger = Logger
                .getLogger(PercentilesByColReducer.class.getName());

        private String[] percentStrs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.percentStrs = context.getConfiguration().get("percent").trim()
                    .split(",");

        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            List<Long> longLists = new ArrayList<Long>();
            for (LongWritable value : values) {
                longLists.add(value.get());
            }
            int longListLength = longLists.size();
            long[] longArrgs = new long[longListLength];
            for (int i = 0; i < longListLength; i++) {
                Long tmpValue = longLists.get(i);
                longArrgs[i] = (tmpValue == null ? 0 : tmpValue);

            }
            if (null != this.percentStrs && 1 == this.percentStrs.length) {

                float percent = Float.parseFloat(this.percentStrs[0]);
                double percentitle = PercenTilesUtil.percenTitle(percent,
                        longArrgs);
                context.write(key, new Text(percentitle + ""));
            }

            if (null != this.percentStrs && 1 < this.percentStrs.length) {

                StringBuilder percentitlesSB = new StringBuilder();
                for (int i = 0; i < this.percentStrs.length; i++) {
                    float percent = Float.parseFloat(this.percentStrs[i]);
                    double percentitle = PercenTilesUtil.percenTitle(percent,
                            longArrgs);
                    percentitlesSB.append(percentitle);
                    if (i < this.percentStrs.length - 1) {
                        percentitlesSB.append("\t");
                    }
                }
                context.write(key, new Text(percentitlesSB.toString()));
            }

        }
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Job job = new Job();
        job.setJarByClass(PercentilesByColMRUTL.class);
        job.setJobName("ProportionByColMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("groupby", "0,1,2");
        job.getConfiguration().set("pncolindex", "3");
        job.getConfiguration().set("percent", "0.75,0.9,0.95");
        FileInputFormat.setInputPaths(job, "fenzi");
        FileOutputFormat.setOutputPath(job, new Path("output_percent"));
        job.setMapperClass(PercentilesByColMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(PercentilesByColReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
