/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DimensionMR.java 
 * @Package com.bi.common.cal 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-7-24 下午5:59:09 
 * @input:输入日志路径/2013-7-24
 * @output:输出日志路径/2013-7-24
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.common.cal;

import java.io.IOException;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: DimensionMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-24 下午5:59:09
 */
public class DimensionMR extends Configured implements Tool {

    public static class DimensionMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> hashmap = new HashMap<String, String>();

        private String[] colNum = null;

        private String distbycolum = null;

        private String delim = null;

        @Override
        public void setup(Context context) {
            colNum = context.getConfiguration().get("column").split(",");
            distbycolum = context.getConfiguration().get("distbycolum");
            this.delim = context.getConfiguration().get("delim");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String colValue = "";
                String[] field = value.toString().trim().split("\t");

                if (1 == colNum.length && -1 == Integer.parseInt(colNum[0])) {
                    colValue = value.toString().trim();
                }
                else {
                    for (int i = 0; i < colNum.length; i++) {
                        colValue += field[Integer.parseInt(colNum[i])] + "\t";
                    }
                }
                StringBuilder distbycolumValueSB = new StringBuilder();
                if (distbycolum.contains(",")) {
                    String[] distbycolums = distbycolum.split(",");

                    for (String diCom : distbycolums) {
                        distbycolumValueSB
                                .append(field[Integer.parseInt(diCom)]
                                        .toUpperCase());
                    }
                }
                else {

                    distbycolumValueSB.append(field[Integer
                            .parseInt(distbycolum)].toUpperCase());
                }
                String distbycolumValue = distbycolumValueSB.toString();

                if (!hashmap.containsKey(distbycolumValue)) {
                    hashmap.put(distbycolumValue, "1");
                    context.write(new Text(distbycolumValue),
                            new Text(colValue));
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

    public static class DimensionCombiner extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text(values.iterator().next()));
        }
    }

    public static class DimensionReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(values.iterator().next()), new Text(""));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("column", args[2]);
        conf.set("distbycolum", args[3]);
        conf.set("delim", args[4]);
        Job job = new Job(conf, "DimensionMRUTL");
        job.setJarByClass(DimensionMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setPartitionerClass(DistinctPartioner.class);
        job.setMapperClass(DimensionMapper.class);
        job.setCombinerClass(DimensionCombiner.class);
        job.setReducerClass(DimensionReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        DimensionArgs distinctArgs = new DimensionArgs();
        int nRet = 0;

        try {
            distinctArgs.init("dimensionmr.jar");
            distinctArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new DimensionMR(),
                distinctArgs.getDistinctParam());
        System.out.println(nRet);

    }

}