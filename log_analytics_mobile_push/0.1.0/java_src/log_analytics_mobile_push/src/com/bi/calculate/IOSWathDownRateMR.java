/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: IOSWathDownRateMR.java 
 * @Package com.bi.calculate 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-30 上午10:12:01 
 * @input:输入日志路径/2013-9-30
 * @output:输出日志路径/2013-9-30
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
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

/**
 * @ClassName: IOSWathDownRateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-30 上午10:12:01
 */
public class IOSWathDownRateMR extends Configured implements Tool {

    public static class IOSWathDownRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private int[] groupByColumns = { 0, 1, 2 };

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = DataFormatUtils.split(line,
                    DataFormatUtils.TAB_SEPARATOR, 0);
            int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                    .ordinal()]);
            String mac = fields[FormatBootStrapEnum.MAC.ordinal()];
            boolean desktop = (btype == 3);
            boolean notice = (btype == 2);
            boolean isNoDesktopAndNotice = !desktop && !notice;
            if (isNoDesktopAndNotice)
                return;
            int totalwatchRecords = 0;
            int desktopWatchRecords = 0;
            int noticeWatchRecords = 0;
            if (desktop) {
                desktopWatchRecords = 1;
                totalwatchRecords = 1;
            }
            else if (notice) {
                noticeWatchRecords = 1;
                totalwatchRecords = 1;
            }
            String outputValue = new StringBuilder().append("")
                    .append(totalwatchRecords)
                    .append(DataFormatUtils.TAB_SEPARATOR)
                    .append(desktopWatchRecords)
                    .append(DataFormatUtils.TAB_SEPARATOR)
                    .append(noticeWatchRecords)
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

    public static class IOSWathDownRateReducer extends
            Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long totalWatchRecords = 0;
            long desktopWatchRecords = 0;
            long noticeWatchRecords = 0;

            Set<String> totalWatchMac = new HashSet<String>();
            Set<String> desktopWatchMac = new HashSet<String>();
            Set<String> noticeWatchMac = new HashSet<String>();
            for (Text value : values) {
                String[] field = DataFormatUtils.split(value.toString(),
                        DataFormatUtils.TAB_SEPARATOR, 0);
                String mac = field[3];
                long currentWatchRecords = Long.parseLong(field[0]);
                totalWatchRecords += currentWatchRecords;
                long currentDesktopWatchRecords = Long.parseLong(field[1]);
                desktopWatchRecords += currentDesktopWatchRecords;
                long currentNoticeWatchRecords = Long.parseLong(field[2]);
                noticeWatchRecords += currentNoticeWatchRecords;
                if (currentWatchRecords == 1)
                    totalWatchMac.add(mac);
                if (currentDesktopWatchRecords == 1)
                    desktopWatchMac.add(mac);
                if (currentNoticeWatchRecords == 1)
                    noticeWatchMac.add(mac);

            }
            StringBuilder value = new StringBuilder().append("");
            value.append(totalWatchRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(totalWatchMac.size());
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(desktopWatchRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(desktopWatchMac.size());
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(noticeWatchRecords);
            value.append(DataFormatUtils.TAB_SEPARATOR);
            value.append(noticeWatchMac.size());
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
        int nRet = ToolRunner.run(new Configuration(), new IOSWathDownRateMR(),
                args);
        System.out.println(nRet);
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
        Job job = new Job(conf);
        job.setJarByClass(IOSWathDownRateMR.class);
        job.setMapperClass(IOSWathDownRateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IOSWathDownRateReducer.class);
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
