package com.bi.WeeklyTrafficReport;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.paramparse.AbstractCommandParamParse;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class VisitWeeklyAnalysis extends Configured implements Tool {

    public static class DateByMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");

            String macStr = fields[PvFormatEnum.FCK.ordinal()];    //client mac,web fck

            String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];  //date

            context.write(new Text(macStr), new Text(dateIdStr));
        }
    }

    public static class DateByMacReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text Key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashSet<String> dateIdSet = new HashSet<String>();

            for (Text val : values) {
                dateIdSet.add(val.toString());
            }

            context.write(new Text(dateIdSet.size() + ""), new IntWritable(1));
        }
    }

    public static class VisitAnalysisMapper extends
            Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class VisitAnalysisReducer extends
            Reducer<Text, Text, Text, Text> {

        private int weeklyUserNum = 0;

        private int weeklyTotalVisitDaysNum = 0;

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            int macSum = 0;
            for (Text val : values) {
                macSum++;
            }
            
            weeklyTotalVisitDaysNum += Integer.parseInt(key.toString())
                    * macSum;
            weeklyUserNum += macSum;
            context.write(key, new Text(macSum + ""));
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            context.write(new Text("avg_visit_days"), new Text(
                    (float)weeklyTotalVisitDaysNum / weeklyUserNum + ""));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        conf = gop.getConfiguration();

        Job job = new Job(conf, "dateByMac distinct");

        
//        for (String path : args[0].split(",")) {
//            FileInputFormat.addInputPath(job, new Path(path));
//        }

        FileInputFormat.addInputPaths(job, args[0]);

        Path tmpOutput = new Path(args[1] + "_tmp");
        FileOutputFormat.setOutputPath(job, tmpOutput);
        tmpOutput.getFileSystem(conf).delete(tmpOutput, true);

        job.setJarByClass(VisitWeeklyAnalysis.class);
        job.setMapperClass(DateByMacMapper.class);
        job.setReducerClass(DateByMacReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(30);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job combineJob = new Job(conf, "visitAnalysis combine");
            combineJob.setJarByClass(VisitWeeklyAnalysis.class);

            FileInputFormat
                    .addInputPath(combineJob, new Path(args[1] + "_tmp"));
            Path outputPath = new Path(args[1]);
            FileOutputFormat.setOutputPath(combineJob, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath, true);  //equals "rm -rf"

            combineJob.setMapperClass(VisitAnalysisMapper.class);
            combineJob.setReducerClass(VisitAnalysisReducer.class);

            combineJob.setInputFormatClass(KeyValueTextInputFormat.class);
            combineJob.setOutputFormatClass(TextOutputFormat.class);
            
            combineJob.setOutputKeyClass(Text.class);
            combineJob.setOutputValueClass(Text.class);

            combineJob.setNumReduceTasks(1);
            code = combineJob.waitForCompletion(true) ? 0 : 1;
        }

        FileSystem.get(conf).delete(new Path(args[1] + "_tmp"), true);

        return code;
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new VisitWeeklyAnalysis(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    static class ParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            return "";
        }

        @Override
        public String getFunctionUsage() {
            return "";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }
}
