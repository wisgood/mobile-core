package com.bi.common.etl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;



public class LogFormatMR {
    
    private static FormatFactory factory=null;

    public static class LogFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(LogFormatMapper.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);

            factory = new FormatFactory();
            String xmlFilename = context.getConfiguration().get("format_xml_file");
            try {
                factory.init(xmlFilename);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";
            try{

            }
            catch(Exception e) {
                logger.error("error originalData:" + fields);
                logger.error(e.getMessage(), e.getCause());
                return empty;
            }
            return empty;

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String formatLog = factory.formatFlow(value.toString());
            context.write(new Text("1"), new Text(formatLog));
        }


    }

    public static class LogFormatReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text());
            }

        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        
        Job job = new Job();
        job.setJarByClass(LogFormatMR.class);
        job.setJobName("LogFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/fbuffer.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/fbuffer"));
        job.setMapperClass(LogFormatMapper.class);
        job.setReducerClass(LogFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
