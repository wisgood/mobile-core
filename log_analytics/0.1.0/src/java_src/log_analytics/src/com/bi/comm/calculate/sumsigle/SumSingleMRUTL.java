package com.bi.comm.calculate.sumsigle;

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

public class SumSingleMRUTL {

    public static class SumSingleMapper extends
            Mapper<LongWritable, Text, NullWritable, LongWritable> {

        private String sumCol = null;

        public void setup(Context context) {
            sumCol = context.getConfiguration().get("sumcol");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] field = value.toString().trim().split("\t");

            try {
                double sumDouble = Double.parseDouble(field[Integer
                        .parseInt(sumCol)]);
                long sumLong = (long) sumDouble;
                context.write(NullWritable.get(), new LongWritable(sumLong));
            }
            catch(Exception e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * reduce : output : sum
     */

    public static class SumSingleReducer extends
            Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {

        public void reduce(NullWritable key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(NullWritable.get(), new LongWritable(sum));
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
        Job job = new Job();
        job.setJarByClass(SumSingleMRUTL.class);
        job.setJobName("SumSingleMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("sumcol", "21");
        FileInputFormat.setInputPaths(job, new Path("output_playtm"));
        FileOutputFormat
                .setOutputPath(job, new Path("output_playtm_sumsingle"));
        job.setMapperClass(SumSingleMapper.class);
        job.setCombinerClass(SumSingleReducer.class);
        job.setReducerClass(SumSingleReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
