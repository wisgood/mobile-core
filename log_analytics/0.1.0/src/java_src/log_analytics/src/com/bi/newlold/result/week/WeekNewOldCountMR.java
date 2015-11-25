package com.bi.newlold.result.week;

import java.io.IOException;
import java.math.BigDecimal;

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
import org.apache.log4j.Logger;

import com.bi.comm.util.CommArgs;

public class WeekNewOldCountMR extends Configured implements Tool {

    public static class WeekNewOldCountMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        enum WeekInfoEnum {

            DATE_ID, MAC_CODE, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, ISLASTWEEK, ISLOST, LOGINDAYS
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length > WeekInfoEnum.LOGINDAYS.ordinal()) {
                String dateId = fields[WeekInfoEnum.DATE_ID.ordinal()];

                context.write(
                        new Text(dateId),
                        new Text(fields[WeekInfoEnum.ISTHISWEEK.ordinal()]
                                + "\t" + fields[WeekInfoEnum.ISNEW.ordinal()]
                                + "\t"
                                + fields[WeekInfoEnum.ISNEWOLD.ordinal()]
                                + "\t"
                                + fields[WeekInfoEnum.ISOLDOLD.ordinal()]
                                + "\t"
                                + fields[WeekInfoEnum.ISOLDNEW.ordinal()]
                                + "\t"
                                + fields[WeekInfoEnum.ISLASTWEEK.ordinal()]
                                + "\t" + fields[WeekInfoEnum.ISLOST.ordinal()]
                                + "\t"
                                + fields[WeekInfoEnum.LOGINDAYS.ordinal()]));
            }
        }

    }

    public static class WeekNewOldCountComber extends
            Reducer<Text, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekNewOldCountComber.class.getName());

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            int weekUserCount = 0;
            int newUserCount = 0;
            int newOldUserCount = 0;
            int oldOldUserCount = 0;
            int oldNewUserCount = 0;
            int lastUserCount = 0;
            int lostUserCount = 0;
            int logindays = 0;

            for (Text value : values) {
                String line = value.toString().trim();

                String[] fields = line.split("\t");
                weekUserCount += Integer.parseInt(fields[0]);
                newUserCount += Integer.parseInt(fields[1]);
                newOldUserCount += Integer.parseInt(fields[2]);
                oldOldUserCount += Integer.parseInt(fields[3]);
                oldNewUserCount += Integer.parseInt(fields[4]);
                lastUserCount += Integer.parseInt(fields[5]);
                lostUserCount += Integer.parseInt(fields[6]);
                logindays += Integer.parseInt(fields[7]);
            }
            context.write(key, new Text(weekUserCount + "\t" + newUserCount
                    + "\t" + newOldUserCount + "\t" + oldOldUserCount + "\t"
                    + oldNewUserCount + "\t" + lastUserCount + "\t"
                    + lostUserCount + "\t" + logindays));

        }
    }

    public static class WeekNewOldCountReducer extends
            Reducer<Text, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekNewOldCountReducer.class.getName());

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            try {
                int weekUserCount = 0;
                int newUserCount = 0;
                int newOldUserCount = 0;
                int oldOldUserCount = 0;
                int oldNewUserCount = 0;
                int lastUserCount = 0;
                int lostUserCount = 0;
                BigDecimal lostRate = null;
                int logindays = 0;
                BigDecimal avgLoginDays = null;
                for (Text value : values) {
                    String line = value.toString().trim();
                    String[] fields = line.split("\t");
                    weekUserCount += Integer.parseInt(fields[0]);
                    newUserCount += Integer.parseInt(fields[1]);
                    newOldUserCount += Integer.parseInt(fields[2]);
                    oldOldUserCount += Integer.parseInt(fields[3]);
                    oldNewUserCount += Integer.parseInt(fields[4]);
                    lastUserCount += Integer.parseInt(fields[5]);
                    lostUserCount += Integer.parseInt(fields[6]);
                    logindays += Integer.parseInt(fields[7]);
                }
                logger.info(weekUserCount + "\t" + newUserCount + "\t"
                        + newOldUserCount + "\t" + oldOldUserCount + "\t"
                        + oldNewUserCount + "\t" + lostUserCount + "\t"
                        + logindays);
                try {
                    lostRate = new BigDecimal(lostUserCount).divide(new BigDecimal(
                            lastUserCount), 3, BigDecimal.ROUND_HALF_DOWN);
                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                logger.info("lostRate" + lostRate);
                avgLoginDays = new BigDecimal(logindays).divide(new BigDecimal(
                        weekUserCount), 3, BigDecimal.ROUND_HALF_DOWN);
                logger.info("avgLoginDays" + avgLoginDays);
                context.write(key, new Text(weekUserCount + "\t" + newUserCount
                        + "\t" + newOldUserCount + "\t" + oldOldUserCount
                        + "\t" + oldNewUserCount + "\t" + lostUserCount + "\t"
                        + lostRate + "\t" + logindays + "\t" + avgLoginDays));
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
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
        commArgs.init("weekusercount.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new WeekNewOldCountMR(),
                commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "WeekNewOldCountMR");
        job.setJarByClass(WeekNewOldCountMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WeekNewOldCountMapper.class);
        job.setCombinerClass(WeekNewOldCountComber.class);
        job.setReducerClass(WeekNewOldCountReducer.class);
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
