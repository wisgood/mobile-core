/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: AppCrashMR.java 
 * @Package com.bi.mobile.crash.format 
 * @Description: 崩溃日志格式化
 * @author fuys
 * @date 2013-8-22 下午3:07:27 
 * @input:输入日志路径/2013-8-22 /dw/logs/mobile/origin/app_error/1/$DIR_DAY
 * @output:输出日志路径/2013-8-22 /dw/logs/mobile/origin/app_error/1/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_mobile_qos.jar  com.bi.format.crash.AppCrashFormatMR  -files /disk6/datacenter/mobile/conf/dm_mobile_platy "-Dinput=/dw/logs/mobile/origin/app_error/1/$DIR_DAY" "-Doutput=/dw/logs/4_mobile_platform/3_quality/app_crash/$DIR_DAY"  "-DexecuteDate=$DATE_ID"  "-Dinpulzo=1"  "-DreduceNum=10"
 * @inputFormat:DEV, VERSION, MAC, TIME, FILEPATH
 * @ouputFormat: DATE_ID, PLAT_ID, VERSION_ID, MAC
 */
package com.bi.format.crash;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;

/**
 * @ClassName: AppCrashMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-22 下午3:07:27
 */
public class AppCrashFormatMR extends Configured implements Tool {

    enum AppCrashEnum {
        /**
         * @设备类型
         * @APP版本号
         * @设备MAC
         * @日期时间
         * @文件名
         */
        DEV, VERSION, MAC, TIME, FILEPATH;

    }

    public static class AppCrashFormatMapper extends
            Mapper<LongWritable, Text, NullWritable, Text> {
        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            File dmMobilePlayFile = new File(CommonConstant.DM_MOBILE_PLATY);
            this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
            System.out.println(multipleOutputs);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String valueStr = value.toString();
            try {
                String[] fields = DataFormatUtils.split(valueStr,
                        DataFormatUtils.COMMA_SEPARATOR, 0);
                if (fields.length > AppCrashEnum.TIME.ordinal()) {

                    // 获取设备类型
                    String platInfo = fields[AppCrashEnum.DEV.ordinal()];
                    platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
                    int platId = dmPlatyRuleDAO.getDMOjb(platInfo);
                    // 版本号
                    String versionInfoStr = fields[AppCrashEnum.VERSION
                            .ordinal()];
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfoStr);

                    // mac地址
                    String macOrgInfoStr = fields[AppCrashEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macOrgInfoStr);
                    String macInforStr = MACFormatUtil
                            .macFormatToCorrectStr(macOrgInfoStr);
                    // 日期
                    String dateInfoStr = fields[AppCrashEnum.TIME.ordinal()];
                    String dateIdStr = dateInfoStr.substring(0,
                            CommonConstant.DATE_FORMAT.length());
                    multipleOutputs.write(NullWritable.get(), new Text(
                            dateIdStr + DataFormatUtils.TAB_SEPARATOR + platId
                                    + DataFormatUtils.TAB_SEPARATOR + versionId
                                    + DataFormatUtils.TAB_SEPARATOR
                                    + macInforStr),
                            CommonConstant.FORMAT_OUTPUT_DIR);

                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(
                        NullWritable.get(),
                        new Text(null == e.getMessage() ? "error" : e
                                .getMessage()
                                + DataFormatUtils.TAB_SEPARATOR
                                + value.toString()),
                        CommonConstant.ERROR_OUTPUT_DIR);

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AppCrashFormatMR(),
                args);
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
        job.setJarByClass(AppCrashFormatMR.class);
        job.setMapperClass(AppCrashFormatMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_appCrashFormatMR_" + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        System.out.println(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
        System.out.println(CommonConstant.REDUCE_NUM + reduceNum);
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
