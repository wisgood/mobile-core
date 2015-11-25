package com.bi.common.etl;

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

import com.bi.common.etl.constant.DMInforHashEnum;

public class InfohashLogFormatMR {
    
    private static FormatFactory factory=null;
    private static Text outKey = new Text();
    private static Text outValue = new Text();
    private static String outputFieldSperator = null;

    public static class InfohashLogFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(InfohashLogFormatMapper.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);

            factory = new FormatFactory();
            String xmlFilename = context.getConfiguration().get("format_xml_file");
            try {
                factory.init(xmlFilename);
                outputFieldSperator = factory.getOutputFieldSperator();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String fields[] = line.split("\t");
            if (fields.length == DMInforHashEnum.CHANNEL_ID.ordinal() + 1) {
                String dimLine = line.replaceAll("\t", ";" + outputFieldSperator);
                outKey.set(fields[DMInforHashEnum.IH.ordinal()].trim()
                        .toLowerCase());
                outValue.set(dimLine);
            } else{
                String infohash = factory.getInfohash(line);
                String formatLog = factory.formatFlow(line);
                outKey.set(infohash);
                outValue.set(formatLog);
                if(infohash == null){
                    return;
                }
            }
            
            context.write(outKey, outValue);
        }
    }

    public static class InfohashLogFormatReducer extends
            Reducer<Text, Text, Text, Text> {
        private int typeId;
        
        private String defalutDMInfohash = "-1" + outputFieldSperator + "-1" + outputFieldSperator + "-1";
        private String defalutDMVideo = "-1" + outputFieldSperator + "-1" + outputFieldSperator + "-1";
        
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            String channelType = context.getConfiguration().get("channel_type");
            typeId = Integer.parseInt(channelType);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String dimStr = null;
            List<String> outLogList = new ArrayList<String>();
            
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(";")) {
                    dimStr = value.replace(";", "");
                } else {
                    outLogList.add(value);
                }
            }
            
            for (String outLogStr : outLogList) {
                
                String outFormatValue = "";
                if (null != dimStr) {
                    outFormatValue = outLogStr + outputFieldSperator + dimStr;
                }
                else {
                    if(typeId == 1){
                        outFormatValue = outLogStr + outputFieldSperator + defalutDMInfohash;
                    }else if(typeId == 2){
                        outFormatValue = outLogStr + outputFieldSperator + defalutDMVideo;
                    }
                }
                context.write(new Text(outFormatValue), new Text(""));
            }

        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        
        Job job = new Job();
        job.setJarByClass(InfohashLogFormatMR.class);
        job.setJobName("LogFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/fbuffer.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/fbuffer"));
        job.setMapperClass(InfohashLogFormatMapper.class);
        job.setReducerClass(InfohashLogFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
