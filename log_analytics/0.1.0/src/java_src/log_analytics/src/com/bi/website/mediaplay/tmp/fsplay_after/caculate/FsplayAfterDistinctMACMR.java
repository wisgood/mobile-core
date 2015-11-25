/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsplayAfterDistinctMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-21 下午1:37:32 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.comm.util.MACFormatUtil;

/**
 * @ClassName: FsplayAfterDistinctMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-21 下午1:37:32
 */
public class FsplayAfterDistinctMACMR {

    public static class FsplayAfterDistinctMACMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> hashmap = new HashMap<String, String>();

        private String delim = null;

        @Override
        public void setup(Context context) {

            this.delim = context.getConfiguration().get("delim");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {

                String[] field = value.toString().trim().split(this.delim);
                if (field.length >= (FsplayAfterEnum.MAC.ordinal() + 1)) {
                    StringBuilder colValueSB = new StringBuilder();
                    String distbycolumValue = MACFormatUtil
                            .macFormatToCorrectStr(field[FsplayAfterEnum.MAC
                                    .ordinal()]);
                    for (int i = 0; i < field.length; i++) {
                        colValueSB.append(field[i]);
                        if (i < field.length - 1) {
                            colValueSB.append("\t");
                        }
                    }
                    if (!hashmap.containsKey(distbycolumValue)) {
                        hashmap.put(distbycolumValue, "1");
                        context.write(new Text(distbycolumValue), new Text(
                                colValueSB.toString()));
                    }
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

    public static class DistinctPartioner extends HashPartitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            // TODO Auto-generated method stub
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class FsplayAfterDistinctMACCombiner extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text(values.iterator().next()));
        }
    }

    public static class FsplayAfterDistinctMACReducer extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(values.iterator().next()), new Text(""));
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

        String inputPaths = "input_playafter_1,input_playafter_2";
        String delim = ",";
        Job job = new Job();
        job.setJarByClass(FsplayAfterDistinctMACMR.class);
        job.setJobName("FsplayAfterDistinctMACMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        Configuration conf = job.getConfiguration();
        conf.set("delim", delim);
        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, new Path(
                "output_playafter_distinct_mac"));
        job.setPartitionerClass(DistinctPartioner.class);
        job.setMapperClass(FsplayAfterDistinctMACMapper.class);
        job.setCombinerClass(FsplayAfterDistinctMACCombiner.class);
        job.setReducerClass(FsplayAfterDistinctMACReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
