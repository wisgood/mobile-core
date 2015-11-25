/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekRetentionNum.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: ����־����д���
 * @author fuys
 * @date 2013-7-20 ����4:33:10 
 * @input:������־·��/2013-7-20
 * @output:�����־·��/2013-7-20
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.dingzi.day.user;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.common.util.DateFormat;

/**
 * @ClassName: WeekRetentionNum
 * @Description: ������һ�仰��������������
 * @author fuys
 * @date 2013-7-20 ����4:33:10
 */
public class WeekRetentionNumMR extends Configured implements Tool {

    private static final String THIS_WEEK = "thisweek";
    private static final String LAST_WEEK = "lastweek";
    public static class WeekRetentionNumMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private boolean isLastWeekData = false;
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePathStr = fileSplit.getPath().getParent().toString();
           // System.out.println(filePathStr);
            String lastWeeksdayStr = context.getConfiguration().get("weeksday")
                    .trim();
            String weekDateid = DateFormat.getDateIdFormPath(filePathStr)
                    .trim();
            //System.out.println(weekDateid);
            if (weekDateid.equalsIgnoreCase(lastWeeksdayStr)) {

                this.isLastWeekData = true;
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String[] fields = value.toString().split("\t");
            if (fields.length > 1) {
                if (this.isLastWeekData) {
                    context.write(new Text(fields[1]), new Text(
                            WeekRetentionNumMR.LAST_WEEK));
                }
                else {

                    context.write(new Text(fields[1]), new Text(
                            WeekRetentionNumMR.THIS_WEEK));
                }
            }

        }
    }

    public static class WeekRetentionNumReduce extends
            Reducer<Text, Text, Text, Text> {
        private String lastWeeksdayStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.lastWeeksdayStr = context.getConfiguration().get("weeksday")
                    .trim();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            boolean isContainThisWeek = false;
            boolean isContainLastWeek = false;
            for (Text value : values) {
                String valueStr = value.toString();
               // System.out.println(valueStr);
                if (valueStr.equalsIgnoreCase(WeekRetentionNumMR.LAST_WEEK)) {
                    isContainLastWeek = true;
                }
                if (valueStr.equalsIgnoreCase(WeekRetentionNumMR.THIS_WEEK)) {
                    isContainThisWeek = true;
                }
            }
            if (isContainThisWeek && isContainLastWeek) {
                context.write(new Text(this.lastWeeksdayStr), key);
            }

        }

    }

    /**
     * 
     * @Title: main
     * @Description: ������һ�仰�����������������
     * @param @param args ����˵��
     * @return void ��������˵��
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        WeekInfoArgs weekInfoArgs = new WeekInfoArgs();
        weekInfoArgs.init("weekretentionnum.jar");
        weekInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new WeekRetentionNumMR(),
                weekInfoArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_weekretentionnum");
        job.setJarByClass(WeekRetentionNumMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("weeksday", args[2]);
        job.setMapperClass(WeekRetentionNumMapper.class);
        job.setReducerClass(WeekRetentionNumReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(24);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
