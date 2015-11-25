package com.bi.newlold.homeserver.extractedhs.format;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.comm.util.CommArgs;
import com.bi.newlold.homeserver.dataenum.HomeServerEnum;

public class ExtractdFormatMRUTL extends Configured implements Tool {

    public static class ExtractdFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        // String dateIdStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            // FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            // String filePathStr = fileInputSplit.getPath().toUri().getPath();
            // this.dateIdStr =
            // DateFormat.getDateIDStrFormFilePath(filePathStr);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = valueStr.split("\t");

            String maccodeStr = fields[HomeServerEnum.MACCODE.ordinal()];
            String logindays = fields[HomeServerEnum.TOTALDAYS.ordinal()];
            context.write(new Text(maccodeStr), new Text(logindays));

        }

    }

    public static class ExtractdFormatReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // TreeSet<Integer> dateTree = new TreeSet<Integer>();
            int sumdays = 0;
            for (Text value : values) {
                // String[] fields = value.toString().trim().split("\t");
                //
                // dateTree.add(new Integer(fields[0]));
                // int days = Integer.parseInt(fields[1]);
                int days = Integer.parseInt(value.toString());
                sumdays += days;
            }
            // context.write(new Text(dateTree.first().toString()),
            // new Text(key.toString() + "\t" + sumdays));
            context.write(key, new Text(sumdays + ""));
        }

    }

    /**
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        CommArgs commArgs = new CommArgs();
        commArgs.init("weekuserdata.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new ExtractdFormatMRUTL(), commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "ExtractdFormatMRUTL");
        job.setJarByClass(ExtractdFormatMRUTL.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ExtractdFormatMapper.class);
        job.setReducerClass(ExtractdFormatReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}