package com.bi.comm.calculate.topn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopNMRUTL {

    public static class TopNMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String[] idColumns;

        public void setup(Context context) throws NumberFormatException {

            try {
                idColumns = context.getConfiguration().get("idcolumns")
                        .split(",");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder mapOutputKey = new StringBuilder();
            String[] line = value.toString().split("\t");
            for (int i = 0; i < idColumns.length; i++) {
                if (i != idColumns.length - 1) {
                    mapOutputKey.append(line[Integer.parseInt(idColumns[i])]
                            + ":");
                }
                else {
                    mapOutputKey.append(line[Integer.parseInt(idColumns[i])]);
                }

            }
            context.write(new Text(mapOutputKey.toString()),
                    new Text(value.toString()));

        }
    }

    public static class TopNReducer extends
            Reducer<Text, Text, NullWritable, Text> {

        public int topn;

        private int topColumn;

        @Override
        protected void setup(Context context) throws NumberFormatException {

            try {
                topColumn = Integer.parseInt(context.getConfiguration().get(
                        "topcolumn"));
                topn = Integer.parseInt(context.getConfiguration().get("topn"));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Text> valueTempContainer = new ArrayList<Text>();
            TreeMap<Integer, Integer> topNReduceResult = new TreeMap<Integer, Integer>(
                    new Comparator<Integer>() {

                        @Override
                        public int compare(Integer first, Integer sercond) {
                            return -first.compareTo(sercond);
                        }
                    });
            for (Text value : values) {
                valueTempContainer.add(new Text(value.toString()));
                String line[] = value.toString().split("\t");
                Integer columnValue = Integer.parseInt(line[topColumn]);
                if (null == topNReduceResult.get(columnValue)) {
                    topNReduceResult.put(columnValue, new Integer(1));

                }
                else {
                    int currentCount = topNReduceResult.get(columnValue);
                    topNReduceResult.put(columnValue, new Integer(
                            currentCount + 1));
                }
                if (topNReduceResult.size() > topn)
                    topNReduceResult.remove(topNReduceResult.lastKey());
            }

            Set<Integer> set = new HashSet<Integer>();
            Iterator<Entry<Integer, Integer>> iterator = topNReduceResult
                    .entrySet().iterator();
            int currentCounter = 0;
            while (iterator.hasNext()) {
                Entry<Integer, Integer> entry = iterator.next();
                currentCounter += entry.getValue();
                set.add(entry.getKey());
                if (currentCounter >= topn)
                    break;

            }
            for (Text value : valueTempContainer) {
                String line[] = value.toString().split("\t");
                Integer columnValue = Integer.parseInt(line[topColumn]);
                if (set.contains(columnValue))
                    context.write(NullWritable.get(),
                            new Text(new Text(value.toString())));

            }
            valueTempContainer.clear();

        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Job job = new Job();
        job.setJobName("TopNMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.setJarByClass(TopNMRUTL.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
