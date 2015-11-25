/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterCountMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.format 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-21 上午10:16:53 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;

/**
 * @ClassName: FsPlayAfterCountMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-21 上午10:16:53
 */
public class FsplayAfterCountMR {

    public static class FsPlayAfterCountMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private static Logger logger = Logger
                .getLogger(FsPlayAfterCountMapper.class.getName());

        private final static IntWritable ONE = new IntWritable(1);

        private String delim = null;

        @Override
        public void setup(Context context) {
            this.delim = context.getConfiguration().get("delim");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String fields = value.toString().trim();
            try {
                StringBuilder colValueSb = new StringBuilder();
                String[] field = fields.split(this.delim);
                if (field.length >= (FsplayAfterEnum.MAC.ordinal() + 1)) {
                    colValueSb.append(MACFormatUtil
                            .macFormatToCorrectStr(field[FsplayAfterEnum.MAC
                                    .ordinal()]));
                    colValueSb.append("\t");
                    colValueSb.append(TimestampFormatUtil.formatTimestamp(
                            field[FsplayAfterEnum.TIMESTAMP.ordinal()]).get(
                            ConstantEnum.DATE_ID));

                    context.write(new Text(colValueSb.toString()), ONE);
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                logger.error("error originalData:" + fields);
                logger.error(e.getMessage(), e.getCause());
            }

        }
    }

    /**
     * reduce : sum
     */

    public static class FsPlayAfterCountReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
         String delim = ",";
         String countInputPaths = "input_playafter_1,input_playafter_2";
//        String delim = "\t";
//        String countInputPaths = "output_playafter_distinct_mac";
        Job job = new Job();
        job.setJarByClass(FsplayAfterCountMR.class);
        job.setJobName("FsPlayAfterCountMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("delim", delim);
        FileInputFormat.setInputPaths(job, countInputPaths);
        FileOutputFormat.setOutputPath(job, new Path(
                "output_playafter_date_mac_count_distinct_mac"));
        job.setMapperClass(FsPlayAfterCountMapper.class);
        job.setCombinerClass(FsPlayAfterCountReducer.class);
        job.setReducerClass(FsPlayAfterCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
