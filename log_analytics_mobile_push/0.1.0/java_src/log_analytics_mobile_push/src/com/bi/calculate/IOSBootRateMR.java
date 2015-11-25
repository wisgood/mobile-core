/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: IOSBootRateMR.java 
 * @Package com.bi.calculate 
 * @Description: IOS bootstrap计算(汇总 角标 通知栏)
 * @author fuys
 * @date 2013-9-26 下午2:44:13 
 * @input:输入日志路径   /dw/logs/4_mobile_platform/5_pushreach/format/app_bootstrap/$DIR_DAY" 
 * @output:输出日志路径   /dw/logs/4_mobile_platform/5_pushreach/result/ios/bootrate/$DIR_DAY
 * @executeCmd:$HADOOP_BIN/hadoop jar log_analytics_mobile_pushreach.jar com.bi.calculate.IOSBootRateMR "-Dinput=/dw/logs/4_mobile_platform/5_pushreach/format/app_bootstrap/$DIR_DAY" "-Doutput=/dw/logs/4_mobile_platform/5_pushreach/result/ios/bootrate/$DIR_DAY" "-DjobName=mobile_Push_IOS_BootStrapRate_$DATE_ID" "-Dinpulzo=0"  "-DreduceNum=3"
 * @inputFormat:DATE_ID HOUR_ID PLAT_ID VERSION_ID  MAC IP  BTYPE   TIMESTAMP
 * @ouputFormat:DateId  totalBootRecords    totalBootUserRecords    desktopBootRecords  desktopBootUserRecords  noticeBootRecords   noticeBootUserRecords    【汇总 角标 通知栏】
 */
package com.bi.calculate;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.DimConstant;
import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.KeyCombinedDimensionUtil;
import com.bi.common.util.MidUtil;

/**
 * @ClassName: IOSBootRateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-26 下午2:44:13
 */
public class IOSBootRateMR extends Configured implements Tool {

    public static class IOSBootRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static final int PLAT_INDEX = 2;

        private int[] groupByColumns = { 0, 1, 2 };

        private MidUtil midUtil;

        private long timespace;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            try {
                String midPath = context.getConfiguration().get(
                        CommonConstant.MIDFILENAME);
                midUtil = MidUtil.getInstance(midPath);
                timespace = context.getConfiguration().getLong(
                        CommonConstant.TIMESPACE, 0);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = DataFormatUtils.split(line,
                    DataFormatUtils.TAB_SEPARATOR, 0);
            int plat = Integer.parseInt(fields[PLAT_INDEX]);
            long timestamp = Long
                    .parseLong(fields[FormatBootStrapEnum.TIMESTAMP.ordinal()]);
            boolean ios = (plat == 3) || (plat == 4);
            if (!ios) {
                return;
            }
            boolean isNOContainInTimeSpaceValues = (null != midUtil
                    && 0 != timespace && !midUtil.containInTimeSpace(plat,
                    timestamp, CommonConstant.TOWHOUR));
            if (isNOContainInTimeSpaceValues) {
                return;

            }
            int totalBootRecords = 0;
            int desktopBootRecords = 0;
            int noticeBootRecords = 0;
            int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                    .ordinal()]);
            String mac = fields[FormatBootStrapEnum.MAC.ordinal()];
            boolean desktop = (btype == 3);
            boolean notice = (btype == 2);
            boolean isNoDesktopAndNotice = !desktop && !notice;
            if (isNoDesktopAndNotice)
                return;
            if (desktop) {
                desktopBootRecords = 1;
            }
            else if (notice) {
                noticeBootRecords = 1;
            }
            totalBootRecords = 1;

            String outputValue = new StringBuilder().append("")
                    .append(totalBootRecords)
                    .append(DataFormatUtils.TAB_SEPARATOR)
                    .append(desktopBootRecords)
                    .append(DataFormatUtils.TAB_SEPARATOR)
                    .append(noticeBootRecords)
                    .append(DataFormatUtils.TAB_SEPARATOR).append(mac)
                    .toString();
            List<String> outKeyList = KeyCombinedDimensionUtil.getOutputKey(
                    fields, groupByColumns, DimConstant.IOS_TOTAL_PLAT);

            for (int i = 0; i < outKeyList.size(); i++) {
                context.write(new Text(outKeyList.get(i)),
                        new Text(outputValue));
            }

        }
    }

    public static class IOSBootRateReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalBootRecords = 0;
            long desktopBootRecords = 0;
            long noticeBootRecords = 0;
            Set<String> totalBootMacSet = new HashSet<String>();
            Set<String> desktopBootSet = new HashSet<String>();
            Set<String> noticeBootMacSet = new HashSet<String>();
            for (Text value : values) {
                String[] fields = DataFormatUtils.split(value.toString(),
                        DataFormatUtils.TAB_SEPARATOR, 0);
                String mac = fields[3];
                long currentBootRecords = Long.parseLong(fields[0]);
                totalBootRecords += currentBootRecords;
                long currentDesktopBootRecords = Long.parseLong(fields[1]);
                desktopBootRecords += currentDesktopBootRecords;
                long currentNoticeBootRecords = Long.parseLong(fields[2]);
                noticeBootRecords += currentNoticeBootRecords;
                if (currentBootRecords == 1) {
                    totalBootMacSet.add(mac);
                }
                if (currentDesktopBootRecords == 1) {
                    desktopBootSet.add(mac);
                }
                if (currentNoticeBootRecords == 1) {
                    noticeBootMacSet.add(mac);
                }
            }
            StringBuilder value = new StringBuilder();
            value.append(totalBootRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(totalBootMacSet.size());
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(desktopBootRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(desktopBootSet.size());
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(noticeBootRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(noticeBootMacSet.size());
            context.write(key, new Text(value.toString()));
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
        int nRet = ToolRunner.run(new Configuration(), new IOSBootRateMR(),
                args);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(IOSBootRateMR.class);
        job.setMapperClass(IOSBootRateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IOSBootRateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String jobName = job.getConfiguration().get("jobName");
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
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
