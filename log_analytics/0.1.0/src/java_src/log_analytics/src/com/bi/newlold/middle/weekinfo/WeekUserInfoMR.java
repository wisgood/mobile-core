package com.bi.newlold.middle.weekinfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.log4j.Logger;

import com.bi.comm.util.DateFormat;
import com.bi.comm.util.DateFormatInfo;

public class WeekUserInfoMR extends Configured implements Tool {
    public static final String NEW_OLD_USER = "newolduser";

    public static class WeekUserInfoMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekUserInfoMapper.class.getName());

        private String weekDateid = null;

        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileSplit.getPath().getParent().toString();
            logger.info("filePathStr:" + this.filePathStr);
            this.weekDateid = DateFormat.getDateIdFormPath(this.filePathStr);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split(DateFormatInfo.SEPARATOR);

            if (this.filePathStr.trim().contains(
                    "f_client_user_newold_week_info")) {
                if ("1".equalsIgnoreCase(fields[3])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoMR.NEW_OLD_USER));
                }

            }
            else {
                context.write(new Text(fields[0]), new Text(this.weekDateid
                        + DateFormatInfo.SEPARATOR + fields[1]));
            }
        }

    }

    public static class WeekUserInfoReducer extends
            Reducer<Text, Text, IntWritable, Text> {

        private static Logger logger = Logger
                .getLogger(WeekUserInfoReducer.class.getName());

        private Integer currentWeekDate = null;

        private List<Integer> pre4WeekDateList = new ArrayList<Integer>(4);

        private List<Integer> pre58WeekDateList = new ArrayList<Integer>(4);

        List<Integer> forDateList = new ArrayList<Integer>(4);

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            String weeksdayStr = context.getConfiguration().get("weeksday");
            logger.info("weeksdayStr:" + weeksdayStr);
            String[] weeksdayStrs = weeksdayStr.split(",");
            Integer[] weeksdayItgs = new Integer[weeksdayStrs.length];
            for (int i = 0; i < weeksdayStrs.length; i++) {
                weeksdayItgs[i] = new Integer(weeksdayStrs[i]);
            }
            Arrays.sort(weeksdayItgs, new Comparator<Integer>() {
                @Override
                public int compare(Integer first, Integer sercond) {
                    return -first.compareTo(sercond);
                }
            });
            this.currentWeekDate = weeksdayItgs[0];
            for (int i = 1; i <= 4; i++) {
                this.forDateList.add(weeksdayItgs[i - 1]);
                this.pre4WeekDateList.add(weeksdayItgs[i]);
                this.pre58WeekDateList.add(weeksdayItgs[i + 4]);
            }

            logger.info("currentweekdate: " + this.currentWeekDate);
            logger.info("forweekdate: " + this.forDateList);
            logger.info("pre4weekdateList:" + this.pre4WeekDateList);
            logger.info("pre58WeekDateList: " + this.pre58WeekDateList);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String maccode = key.toString();
            // 本周登录用户
            int currentUser = 0;
            // 本周登录新用户
            int currentNewUser = 0;
            // 本周新老用户
            int currentNewOldUser = 0;
            // 本周登录老用户
            int oldoldUser = 0;
            // 本周老新用户数
            int oldnewUser = 0;
            // 前第4周登录用户
            int pre4thUser = 0;
            // 前第四周流失用户
            int pre4thlostUsr = 0;
            //用户启动天数
            int currentloginDay = 0;
          
            // 前第4周日期
            Integer prethDateid = this.pre4WeekDateList
                    .get(this.pre4WeekDateList.size() - 1);
            List<Integer> loginDateList = new ArrayList<Integer>(20);
            // 本周登录天数
            Map<String, String> macToLoginDay = new HashMap<String, String>();
            // 本周新老用户数
            Map<String, Boolean> macToOldNew = new HashMap<String, Boolean>();
            for (Text value : values) {
                String line = value.toString();
                String[] fields = line.split(DateFormatInfo.SEPARATOR);
                Integer dateId = Integer.parseInt(fields[0]);
                loginDateList.add(dateId);
                String loginDateStr = fields[1];
                if (loginDateStr.equalsIgnoreCase(WeekUserInfoMR.NEW_OLD_USER)) {
                    macToOldNew.put(maccode, new Boolean(true));
                }

                if (dateId.equals(this.currentWeekDate)) {
                    currentloginDay = Integer.parseInt(fields[1]);
                }
                macToLoginDay.put(maccode, currentloginDay + "");

            }
            if (loginDateList.contains(prethDateid)) {

                pre4thUser = 1;

            }

            Integer[] loginDates = new Integer[loginDateList.size()];
            loginDates = loginDateList.toArray(loginDates);
            Arrays.sort(loginDates, new Comparator<Integer>() {
                @Override
                public int compare(Integer first, Integer sercond) {
                    return -first.compareTo(sercond);
                }
            });

            // StringBuilder weekInfoSb = new StringBuilder();
            if (loginDates[0].equals(this.currentWeekDate)) {

                int count = 3;
                currentUser = 1;
                // 本周新用户 前8周都不存在
                if (!WeekInfo.isArraysContainInList(loginDates,
                        this.pre4WeekDateList)
                        && !WeekInfo.isArraysContainInList(loginDates,
                                this.pre58WeekDateList)) {
                    currentNewUser = 1;
                    count--;

                }
                // 本周新老用户数
                if (null != macToOldNew.get(maccode)
                        && macToOldNew.get(maccode).booleanValue()) {

                    currentNewOldUser = 1;
                    count--;
                }
                // 老老用户
                if (WeekInfo.isArraysContainInList(loginDates,
                        this.pre4WeekDateList)
                        && (WeekInfo.isArraysContainInList(loginDates,
                                this.pre58WeekDateList))) {
                    oldoldUser = 1;
                    count--;

                }

                if (count == 3) {
                    oldnewUser = 1;

                }

            }

            if (loginDates[0].equals(prethDateid)) {
                pre4thlostUsr = 1;
            }

            context.write(new IntWritable(this.currentWeekDate), new Text(
                    maccode + "\t" + currentUser + "\t" + currentNewUser + "\t"
                            + currentNewOldUser + "\t" + oldoldUser + "\t"
                            + oldnewUser + "\t" + pre4thUser + "\t" + pre4thlostUsr
                            + "\t" + macToLoginDay.get(maccode)));

        }
    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        WeekInfoArgs weekInfoArgs = new WeekInfoArgs();
        weekInfoArgs.init("weeknewUser.jar");
        weekInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new WeekUserInfoMR(),
                weekInfoArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "WeekUserInfoMR");
        job.setJarByClass(WeekUserInfoMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("weeksday", args[2]);
        job.setMapperClass(WeekUserInfoMapper.class);
        job.setReducerClass(WeekUserInfoReducer.class);
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
