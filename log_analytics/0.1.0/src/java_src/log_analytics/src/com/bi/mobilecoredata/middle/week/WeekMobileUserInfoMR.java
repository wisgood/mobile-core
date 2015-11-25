/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMobileUserInfoMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周新老用户列表
 * @author fuys
 * @date 2013-7-5 下午4:48:06 
 * @input:输入日志路径/2013-7-5   9周的/dw/logs/mobile/result/week/weekmac/和前1-4周的/dw/logs/mobile/result/week/f_mobile_user_newold_week_info
 * @output:输出日志路径/2013-7-5
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMobileUserInfoMR --input $DIR_ORIGINDATA_WEEKINFO_INPUT --output /dw/logs/mobile/result/week/f_mobile_user_newold_week_info/$DIR_WEEKDATE  --weeksday $PARS_WEEKDATE_IDS
 * @inputFormat:DATE_ID QUDAO PLAT MAC LOGINDAYS和DATE_ID QUDAO PLAT MAC  ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW LOGINDAYS
 * @ouputFormat:DATE_ID QUDAO PLAT MAC  ISTHISWEEK ISNEW ISNEWOLD ISOLDOLD ISOLDNEW LOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

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
import com.bi.newlold.middle.weekinfo.WeekInfo;
import com.bi.newlold.middle.weekinfo.WeekInfoArgs;

/**
 * @ClassName: WeekMobileUserInfoMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-5 上午11:21:42
 */
public class WeekMobileUserInfoMR extends Configured implements Tool {
    public static final String NEW_OLD_USER = "newolduser";

    public static class WeekMobileUserInfoMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        /**
         * the input log form
         */
        private enum Log {
            DATE_ID, QUDAO, PLAT, MAC, LOGINDAYS
        }

        enum WeekInfoEnum {

            DATE_ID, QUDAO, PLAT, MAC, ISTHISWEEK, ISNEW, ISNEWOLD, ISOLDOLD, ISOLDNEW, LOGINDAYS
        }

        private static Logger logger = Logger
                .getLogger(WeekMobileUserInfoMapper.class.getName());

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
                    "f_mobile_user_newold_week_info")) {
                if ("1".equalsIgnoreCase(fields[WeekInfoEnum.ISNEW.ordinal()])) {
                    String keyStr = fields[WeekInfoEnum.QUDAO.ordinal()]
                            + DateFormatInfo.SEPARATOR
                            + fields[WeekInfoEnum.PLAT.ordinal()]
                            + DateFormatInfo.SEPARATOR
                            + fields[WeekInfoEnum.MAC.ordinal()];
                    context.write(new Text(keyStr), new Text(this.weekDateid
                            + DateFormatInfo.SEPARATOR
                            + WeekMobileUserInfoMR.NEW_OLD_USER));
                }
            }
            else {
                if (fields.length > Log.LOGINDAYS.ordinal()) {
                    String keyStr = fields[Log.QUDAO.ordinal()]
                            + DateFormatInfo.SEPARATOR
                            + fields[Log.PLAT.ordinal()]
                            + DateFormatInfo.SEPARATOR
                            + fields[Log.MAC.ordinal()];
                    context.write(new Text(keyStr),
                            new Text(this.weekDateid + DateFormatInfo.SEPARATOR
                                    + fields[Log.LOGINDAYS.ordinal()]));
                }
            }
        }

    }

    public static class WeekMobileUserInfoReducer extends
            Reducer<Text, Text, IntWritable, Text> {

        private static Logger logger = Logger
                .getLogger(WeekMobileUserInfoReducer.class.getName());

        private Integer currentWeekDate = null;

        private List<Integer> pre4WeekDateList = new ArrayList<Integer>(4);

        private List<Integer> pre58WeekDateList = new ArrayList<Integer>(4);

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

                this.pre4WeekDateList.add(weeksdayItgs[i]);
                this.pre58WeekDateList.add(weeksdayItgs[i + 4]);
            }
            logger.info("currentweekdate: " + this.currentWeekDate);
            logger.info("pre4weekdateList:" + this.pre4WeekDateList);
            logger.info("pre58WeekDateList: " + this.pre58WeekDateList);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String keValueInfoStr = key.toString();
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
            // 用户启动天数
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
                //logger.info("loginDateStr:" + loginDateStr);
                if (loginDateStr
                        .equalsIgnoreCase(WeekMobileUserInfoMR.NEW_OLD_USER)) {
                    macToOldNew.put(keValueInfoStr, new Boolean(true));
                   // logger.info("newold:" + loginDateStr);
                }

                if (dateId.equals(this.currentWeekDate)) {
                    currentloginDay = Integer.parseInt(fields[1]);
                }
                macToLoginDay.put(keValueInfoStr, currentloginDay + "");

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
                if (null != macToOldNew.get(keValueInfoStr)
                        && macToOldNew.get(keValueInfoStr).booleanValue()) {

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
            context.write(new IntWritable(this.currentWeekDate),
                    new Text(keValueInfoStr + DateFormatInfo.SEPARATOR
                            + currentUser + DateFormatInfo.SEPARATOR
                            + currentNewUser + DateFormatInfo.SEPARATOR
                            + currentNewOldUser + DateFormatInfo.SEPARATOR
                            + oldoldUser + DateFormatInfo.SEPARATOR
                            + oldnewUser + DateFormatInfo.SEPARATOR
                            + macToLoginDay.get(keValueInfoStr)));
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
        WeekInfoArgs weekInfoArgs = new WeekInfoArgs();
        weekInfoArgs.init("weekmobilenewUser.jar");
        weekInfoArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new WeekMobileUserInfoMR(), weekInfoArgs.getCommsParam());
        System.out.println(res);
    }

    /**
     * (非 Javadoc)
     * <p>
     * Title: run
     * </p>
     * <p>
     * Description:
     * </p>
     * 
     * @param args
     * @return
     * @throws Exception
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekmobileuserinfomr");
        job.setJarByClass(WeekMobileUserInfoMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("weeksday", args[2]);
        job.setMapperClass(WeekMobileUserInfoMapper.class);
        job.setReducerClass(WeekMobileUserInfoReducer.class);
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
