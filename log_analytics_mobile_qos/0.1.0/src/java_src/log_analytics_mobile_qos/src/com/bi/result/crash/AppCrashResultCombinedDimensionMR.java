/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: AppCrashResultCombinedDimensionMR.java 
 * @Package com.bi.result.crash 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-9 下午4:46:30 
 * @input:输入日志路径/2013-9-9
 * @output:输出日志路径/2013-9-9
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.result.crash;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import com.bi.common.constant.CommonConstant;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.KeyCombinedDimensionUtil;

/**
 * @ClassName: AppCrashResultCombinedDimensionMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-9 下午4:46:30
 */
public class AppCrashResultCombinedDimensionMR extends Configured implements
        Tool {

    public static String CRASH = "crash";

    public static String BOOTSTRAP = "bootstrap";

    enum AppCrashResultEnum {
        /**
         * @日期
         * @设备类型ID
         * @APP版本号ID
         * @设备MAC
         * 
         */
        DATE_ID, PLAT_ID, VERSION_ID, MAC;

    }

    enum BootStrapFormatEnum {
        /*
         * 设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号> 
         * 设备mac地址(mac)：长度为16的大写字符串（待确认）  app版本号(ver)：类ip地址的字符串 
         * 网络类型(nt)：1—wifi，2--3g，3—其它 ，-1—无网络  启动方式（btype）:*
         * 0—其它启动；1—手动启动；2—ios平台：推送启动
         * ，android平台：调用播放器播放本地文件；3—ios平台：有角标启动上报（其它启动不包括有角标启动上报
         * ），android平台：推送通知栏启动；4
         * —android平台：后台下载进入下载管理界面；5—android平台：网页调起app；6—android平台
         * ：按home键应用进入后台后再次回到前台
         * ；7—android平台：推送桌面弹窗启动；8—android平台：本地通知启动；9–android平台：通过引入第三方push
         * sdk创建的通知栏启动 10 –android平台：通过引入第三方push sdk创建的桌面弹窗启动 
         * 启动耗时（btime）：从点击到主框架加载完毕耗时，单位：ms  启动是否成功（ok）：1—成功，-1—其它—错误代码 
         * 屏幕分辨率（sr）：屏幕分辨率，N*M  设备内存空间（mem）：单位MB  设备存储空间（tdisk）：单位MB 
         * 设备剩余空间(fdisk)：单位MB  渠道ID(sid):区分各个渠道商  启动时间戳（rt）：unix时间戳 (ipad,
         * iphone) 是否越狱（broken）： (iphone)  设备IMEI（imei）: 设备IMEI号 (aphone，apad)
         * 启动是否成功(OK_TYPE):同OK字段一样 版本号前两位(VERSION_STR)
         */
        DATE_ID, HOUR_ID, PLAT_ID, VERSION_ID, QUDAO_ID, BOOT_TYPE, MACCLEAN, PROVINCE_ID, CITY_ID, ISP_ID, OK_TYPE, VERSION_STR, TIMESTAMP, IP, DEV, MAC, VER, NT, BTYPE, BTIME, OK, SR, MEM, TDISK, FDISK, SID, RT, IPHONEIP, BROKEN, IMEI, INSTALLT, FUDID;
    }

    public static class AppCrashResultCombinedDimensionMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileInputSplit.getPath().getParent().toString();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String lineStr = value.toString();
            String[] fields = DataFormatUtils.split(lineStr,
                    DataFormatUtils.TAB_SEPARATOR, 0);
            // 崩溃日志
            if (this.filePathStr.contains(CRASH)) {
                int[] groupByColumns = { 0, 1, 2 };
                List<String> outKeyList = KeyCombinedDimensionUtil
                        .getOutputKey(fields, groupByColumns);
                for (int i = 0; i < outKeyList.size(); i++) {
                    String outKeyStr = outKeyList.get(i).substring(9).trim();
                    context.write(new Text(outKeyStr), new Text(CRASH
                            + fields[AppCrashResultEnum.MAC.ordinal()]));
                }
            }
            // 启动日志
            else if (this.filePathStr.contains(BOOTSTRAP)) {
                int[] groupByColumns = { 0, 2, 3 };
                List<String> outKeyList = KeyCombinedDimensionUtil
                        .getOutputKey(fields, groupByColumns);
                for (int i = 0; i < outKeyList.size(); i++) {
                    String outKeyStr = outKeyList.get(i).substring(9).trim();
                    context.write(new Text(outKeyStr), new Text(BOOTSTRAP
                            + fields[BootStrapFormatEnum.MACCLEAN.ordinal()]));
                }

            }
        }

    }

    public static class AppCrashResultCombinedDimensionReducer extends
            Reducer<Text, Text, Text, Text> {

        private String dateId;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.dateId = context.getConfiguration().get(
                    CommonConstant.EXECUTE_DATE);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            long crashCount = 0;
            long bootCount = 0;
            HashSet<String> crashHashSet = new HashSet<String>();
            HashSet<String> bootHashSet = new HashSet<String>();
            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.contains(CRASH)) {
                    crashCount++;
                    crashHashSet.add(valueStr);

                }
                if (valueStr.contains(BOOTSTRAP)) {
                    bootCount++;
                    bootHashSet.add(valueStr);
                }
            }
            String valueStr = "" + crashCount + DataFormatUtils.TAB_SEPARATOR
                    + bootCount + DataFormatUtils.TAB_SEPARATOR
                    + crashHashSet.size() + DataFormatUtils.TAB_SEPARATOR
                    + bootHashSet.size();
            if (bootCount > 0) {
                context.write(new Text(this.dateId
                        + DataFormatUtils.TAB_SEPARATOR + key.toString()),
                        new Text(valueStr));

            }

        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new AppCrashResultCombinedDimensionMR(), args);
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
        for (int i = 0; i < args.length; i++) {

            System.out.println(i + ":" + args[i]);
        }
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(AppCrashResultCombinedDimensionMR.class);
        job.setMapperClass(AppCrashResultCombinedDimensionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(AppCrashResultCombinedDimensionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_appCrashResultCombinedDimensionMR_"
                + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        System.out.println(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
        System.out.println(CommonConstant.REDUCE_NUM + ":" + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job.getConfiguration().getInt(
                CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.waitForCompletion(true);
        return 0;
    }

}
