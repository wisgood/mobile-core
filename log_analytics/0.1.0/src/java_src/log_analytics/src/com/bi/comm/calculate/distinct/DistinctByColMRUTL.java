package com.bi.comm.calculate.distinct;

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

public class DistinctByColMRUTL {

    public static class DistinctByColMapper extends
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

    public static class DistinctByColCombiner extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text(values.iterator().next()));
        }
    }

    public static class DistinctByColReduce extends
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
        String resultColumn = "-1";
        String distinctDownloadInput = "output_download_new_outih";
        String distinctDownloadOutput = "output_distinctmac_download";
        String distinctByColumn = "6";
        String delim = "\t";
        Job job = new Job();
        job.setJarByClass(DistinctByCol.class);
        job.setJobName("DistinctMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        Configuration conf = job.getConfiguration();
        conf.set("column", resultColumn);
        conf.set("distbycolum", distinctByColumn);
        conf.set("delim", delim);
        FileInputFormat.setInputPaths(job, distinctDownloadInput);
        FileOutputFormat.setOutputPath(job, new Path(distinctDownloadOutput));
        job.setPartitionerClass(DistinctPartioner.class);
        job.setMapperClass(DistinctByColMapper.class);
        job.setCombinerClass(DistinctByColCombiner.class);
        job.setReducerClass(DistinctByColReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
