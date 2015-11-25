package com.bi.comm.calculate.bufferindex;

import java.io.IOException;

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

public class BufferIndexMR {

    public static class BufferIndexMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String[] idColumns;

        private String[] indexColumns;

        public void setup(Context context) throws NumberFormatException {

            try {
                idColumns = context.getConfiguration().get("idcolumns")
                        .split(",");
                //指标列
                indexColumns = context.getConfiguration().get("indexcolumns")
                        .split(",");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            int btmFormat = Integer.parseInt(fields[Integer
                    .parseInt(indexColumns[0])]);
            String okType = fields[Integer.parseInt(indexColumns[1])];

            int success = 0;
            int fail = 0;
            if (bufferOK(okType)) {
                success = 1;
                fail = 0;
            }
            else {
                success = 0;
                fail = 1;
                btmFormat = 0;
            }
            StringBuilder mapOutputKey = new StringBuilder();
            for (int i = 0; i < idColumns.length; i++) {
                if (i != idColumns.length - 1) {
                    mapOutputKey.append(fields[Integer.parseInt(idColumns[i])]
                            + "\t");
                }
                else {
                    mapOutputKey.append(fields[Integer.parseInt(idColumns[i])]);
                }

            }

            StringBuffer mapOutputValue = new StringBuffer();
            mapOutputValue.append(btmFormat);
            mapOutputValue.append("\t");
            mapOutputValue.append(success);
            mapOutputValue.append("\t");
            mapOutputValue.append(fail);
            mapOutputValue.append("\t");
            context.write(new Text(mapOutputKey.toString()), new Text(
                    mapOutputValue.toString()));

        }

        private boolean bufferOK(String okType) {
            if (null == okType)
                return false;
            try {
                return Integer.parseInt(okType) == 0 ? true : false;

            }
            catch(Exception e) {
                return false;
            }
        }

    }

    public static class BufferIndexReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long successSum = 0;
            long failSum = 0;
            long bTimeSum = 0;
            for (Text value : values) {
                String[] field = value.toString().split("\t");
                long bTime = Integer.parseInt(field[0]);
                long success = Integer.parseInt(field[1]);
                long fail = Integer.parseInt(field[2]);
                successSum += success;
                failSum += fail;
                bTimeSum += bTime;

            }
            // btime,successsum,failsum
            StringBuilder value = new StringBuilder();
            value.append(bTimeSum);
            value.append("\t");
            value.append(successSum);
            value.append("\t");
            value.append(successSum + failSum);
            value.append("\t");
            context.write(key, new Text(value.toString()));

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

        Job job = new Job();
        job.setJobName("BufferIndexMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.setJarByClass(BufferIndexMR.class);
        FileInputFormat.setInputPaths(job, new Path("mq_output_fbuffer"));
        FileOutputFormat.setOutputPath(job, new Path("output_index"));
        job.setMapperClass(BufferIndexMapper.class);
        job.setReducerClass(BufferIndexReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
