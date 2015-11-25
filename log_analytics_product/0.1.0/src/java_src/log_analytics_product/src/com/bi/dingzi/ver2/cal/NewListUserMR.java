/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: HistoryListUserMR.java 
 * @Package com.bi.dingzi.ver2.cal 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-1 下午5:04:13 
 * @input:输入日志路径/2013-8-1
 * @output:输出日志路径/2013-8-1
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.dingzi.ver2.cal;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.dingzi.day.user.UserNewListMR;
import com.bi.dingzi.day.user.WeekCountByWeekDayArgs;

/**
 * @ClassName: HistoryListUserMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-1 下午5:04:13
 */
public class NewListUserMR extends Configured implements Tool {

    public static String HISTORY_USER = "history";

    public static String NEW_USER = "distinct_mac_list";

    public static class HistoryListUserMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        private String dateStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            filePathStr = fileInputSplit.getPath().getParent().toString();
            this.dateStr = context.getConfiguration().get("date");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {

            String line = value.toString().trim();
            String field[] = line.split("\t");
            // 历史用户
            if (this.filePathStr.contains(NewListUserMR.HISTORY_USER)) {
                String mac = field[1].toUpperCase();
                context.write(new Text(mac), new Text(
                        NewListUserMR.HISTORY_USER + dateStr));
            }
            // 每日用户数
            else if (this.filePathStr.contains(NewListUserMR.NEW_USER)) {
                if (field.length > 1) {
                    String mac = field[1].toUpperCase();
                    context.write(new Text(mac), new Text(
                            NewListUserMR.NEW_USER + field[0]));
                }
            }

        }
    }

    public static class HistoryListUserReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String newout = null;

            boolean label = false;
            boolean exLabel = false;
            for (Text val : values) {
                String value = val.toString();
                if (value.contains(UserNewListMR.HISTORY_USER)) {
                    label = true;
                }
                else {
                    exLabel = true;
                    newout = val.toString();
                }
            }
            if (null != newout) {
                if (!label && exLabel) {
                    context.write(
                            new Text(newout.trim().substring(
                                    UserNewListMR.NEW_USER.length())),
                            NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        WeekCountByWeekDayArgs dateInfoArgs = new WeekCountByWeekDayArgs();
        dateInfoArgs.init("newlistuser.jar");
        dateInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new NewListUserMR(),
                dateInfoArgs.getCountParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_newlistuser");
        job.setJarByClass(NewListUserMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("date", args[2]);
        job.setMapperClass(HistoryListUserMapper.class);
        job.setReducerClass(HistoryListUserReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(24);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
