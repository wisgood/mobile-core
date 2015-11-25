/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekUserInfoDetailMR.java 
 * @Package com.bi.newlold.middle.weekinfo 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-7-4 上午9:21:19 
 */
package com.bi.newlold.middle.weekinfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * @ClassName: WeekUserInfoDetailMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-4 上午9:21:19
 */
public class WeekUserInfoDetailMR extends Configured implements Tool {

    public static final String CURRENT_USER = "currentuser";

    public static final String NEW_USER = "newuser";

    public static final String NEW_OLD_USER = "newolduser";

    public static final String OLD_OLD_USER = "oldolduser";

    public static final String OLD_NEW_USER = "oldnewuser";

    enum WeekInfoEnum {

        DATE_ID, MAC_CODE, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, ISLASTWEEK, ISLOST, LOGINDAYS
    }

    public static class WeekUserInfoDetailMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekUserInfoDetailMapper.class.getName());
        private String filePathStr = null;
        public String weekDateid = null;

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
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISTHISWEEK
                        .ordinal()])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoDetailMR.CURRENT_USER + "\t"
                            + fields[WeekInfoEnum.LOGINDAYS.ordinal()]));
                }
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISNEW.ordinal()])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoDetailMR.NEW_USER));
                }
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISNEWOLD.ordinal()])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoDetailMR.NEW_OLD_USER));
                }
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISOLDOLD.ordinal()])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoDetailMR.OLD_OLD_USER));
                }
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISOLDNEW.ordinal()])) {
                    context.write(new Text(fields[1]), new Text(this.weekDateid
                            + "\t" + WeekUserInfoDetailMR.OLD_NEW_USER));
                }
            }
            else {
                context.write(new Text(fields[0]), new Text(this.weekDateid));
            }
        }

    }

    public static class WeekUserInfoDetailReducer extends
            Reducer<Text, Text, IntWritable, Text> {

        private static Logger logger = Logger
                .getLogger(WeekUserInfoDetailReducer.class.getName());

        private Integer currentWeekDate = null;

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
                    return first.compareTo(sercond);
                }
            });
            this.currentWeekDate = weeksdayItgs[0];
            logger.info("currentweekdate: " + this.currentWeekDate);

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
            int currentOldOldUser = 0;
            // 本周老新用户数
            int currentOldNewUser = 0;

            // 本周流失用户
            int currentLostUser = 0;
            // 本周新用户流失
            int currentNewLostUser = 0;
            // 本周新老用户流失
            int currentNewOldLostUser = 0;
            // 本周老老用户流失
            int currentOldOldLostUser = 0;
            // 本周老新用户流失
            int currentOldNewLostUser = 0;

            // 本周用户启动天数
            int currentLoginDays = 0;
            // 本周新用户启动天数
            int currentNewLoginDays = 0;
            // 本周新老用户启动天数
            int currentNewOldLoginDays = 0;
            // 本周老老用户流启动天数
            int currentOldOldLoginDays = 0;
            // 本周老新用户流启动天数
            int currentOldNewLoginDays = 0;

            List<Integer> loginDateList = new ArrayList<Integer>(20);
            // 本周登录天数
            Map<String, Integer> macToLoginDayMap = new HashMap<String, Integer>();
            // 本周新用户
            Map<String, Boolean> macToNewMap = new HashMap<String, Boolean>();
            // 本周新老用户
            Map<String, Boolean> macToNewOldMap = new HashMap<String, Boolean>();
            // 本周老老用户
            Map<String, Boolean> macToOldOldMap = new HashMap<String, Boolean>();
            // 本周老新用户
            Map<String, Boolean> macToOldNewMap = new HashMap<String, Boolean>();
            for (Text value : values) {
                String line = value.toString();
                String[] fields = line.split(DateFormatInfo.SEPARATOR);
                Integer dateId = Integer.parseInt(fields[0]);
                loginDateList.add(dateId);
                if (fields.length > 1) {
                    String loginDateStr = fields[1];
                    if (loginDateStr
                            .equalsIgnoreCase(WeekUserInfoDetailMR.NEW_USER)) {
                        macToNewMap.put(maccode, new Boolean(true));
                    }

                    if (loginDateStr
                            .equalsIgnoreCase(WeekUserInfoDetailMR.NEW_OLD_USER)) {
                        macToNewOldMap.put(maccode, new Boolean(true));
                    }
                    if (loginDateStr
                            .equalsIgnoreCase(WeekUserInfoDetailMR.OLD_OLD_USER)) {
                        macToOldOldMap.put(maccode, new Boolean(true));
                    }
                    if (loginDateStr
                            .equalsIgnoreCase(WeekUserInfoDetailMR.OLD_NEW_USER)) {
                        macToOldNewMap.put(maccode, new Boolean(true));
                    }

                    if (fields.length == 3) {
                        currentLoginDays = Integer.parseInt(fields[2]);

                    }
                    macToLoginDayMap
                            .put(maccode, new Integer(currentLoginDays));
                }

            }
            Integer[] loginDates = new Integer[loginDateList.size()];
            loginDates = loginDateList.toArray(loginDates);
            Arrays.sort(loginDates, new Comparator<Integer>() {
                @Override
                public int compare(Integer first, Integer sercond) {
                    return first.compareTo(sercond);
                }
            });
            //logger.info(Arrays.asList(loginDates));
            if (loginDates[0].equals(this.currentWeekDate)) {
                currentUser = 1;
                currentLostUser = this.getLostCount(loginDates,
                        this.currentWeekDate);
                if (null != macToNewMap.get(maccode)
                        && macToNewMap.get(maccode).booleanValue()) {
                    currentNewUser = 1;
                    currentNewLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentNewLoginDays = macToLoginDayMap.get(maccode)
                            .intValue();

                }
                if (null != macToNewOldMap.get(maccode)
                        && macToNewOldMap.get(maccode).booleanValue()) {
                    currentNewOldUser = 1;
                    currentNewOldLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentNewOldLoginDays = macToLoginDayMap.get(maccode)
                            .intValue();

                }
                if (null != macToOldOldMap.get(maccode)
                        && macToOldOldMap.get(maccode).booleanValue()) {
                    currentOldOldUser = 1;
                    currentOldOldLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentOldOldLoginDays = macToLoginDayMap.get(maccode)
                            .intValue();

                }
                if (null != macToOldNewMap.get(maccode)
                        && macToOldNewMap.get(maccode).booleanValue()) {
                    currentOldNewUser = 1;
                    currentOldNewLostUser = this.getLostCount(loginDates,
                            this.currentWeekDate);
                    currentOldNewLoginDays = macToLoginDayMap.get(maccode)
                            .intValue();

                }

            }
            if (currentUser == 1) {
                context.write(new IntWritable(this.currentWeekDate), new Text(
                        maccode + "\t" + currentUser + "\t" + currentNewUser
                                + "\t" + currentNewOldUser + "\t"
                                + currentOldOldUser + "\t" + currentOldNewUser
                                + "\t" + currentLostUser + "\t"
                                + currentNewLostUser + "\t"
                                + currentNewOldLostUser + "\t"
                                + currentOldOldLostUser + "\t"
                                + currentOldNewLostUser + "\t"
                                + currentLoginDays + "\t" + currentNewLoginDays
                                + "\t" + currentNewOldLoginDays + "\t"
                                + currentOldOldLoginDays + "\t"
                                + currentOldNewLoginDays));
            }
        }

        private int getLostCount(Integer[] loginDates, Integer currentDateId) {
            for (int i = 0; i < loginDates.length; i++) {
                if (!loginDates[i].equals(currentDateId)) {
                    return 0;
                }
            }
            return 1;
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
        weekInfoArgs.init("weekUserInfoDetail.jar");
        weekInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new WeekUserInfoDetailMR(), weekInfoArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "WeekUserInfoDetailMR");
        job.setJarByClass(WeekUserInfoDetailMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("weeksday", args[2]);
        job.setMapperClass(WeekUserInfoDetailMapper.class);
        job.setReducerClass(WeekUserInfoDetailReducer.class);
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
