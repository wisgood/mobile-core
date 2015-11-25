/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMACJoinHisMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 周mac与历史关联取出最早存在的渠道信息
 * @author fuys
 * @date 2013-7-23 上午9:55:47 
 * @input:输入日志路径/2013-7-23 /dw/logs/mobile/result/week/weekmac和/dw/logs/mobile/result/dayuser/history/周日日期
 * @output:输出日志路径/2013-7-23/dw/logs/mobile/result/week/weekmacjoinhis
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMacMR --input $DIR_COREDATA_DAY_DTAIL_INPUT --output /dw/logs/mobile/result/week/weekmacjoinhis/$DIR_WEEKDATE
 * @inputFormat:MAC     QUDAO   PLAT和DATE_ID     QUDAO   PLAT    MAC     LOGINDAYS
 * @ouputFormat:DATE_ID     QUDAO   PLAT    MAC     LOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

import java.io.IOException;

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
import org.apache.log4j.Logger;

import com.bi.comm.util.CommArgs;
import com.bi.comm.util.DateFormatInfo;

/**
 * @ClassName: WeekMACJoinHisMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-23 上午9:55:47
 */
public class WeekMACJoinHisMR extends Configured implements Tool {
    public static final String HISTORY = "history";

    /**
     * the input log form
     */
    private enum WeekMacEnum {
        DATE_ID, QUDAO, PLAT, MAC, LOGINDAYS
    }

    private enum HistMacEnum {
        MAC, QUDAO, PLAT

    }

    public static class WeekMACJoinHisMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        private static Logger logger = Logger
                .getLogger(WeekMACJoinHisMapper.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileSplit.getPath().getParent().toString();
            logger.info("filePathStr:" + this.filePathStr);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String line = value.toString();
            String[] fields = line.split(DateFormatInfo.SEPARATOR);
            if (fields.length > HistMacEnum.PLAT.ordinal()) {

                if (this.filePathStr.contains(WeekMACJoinHisMR.HISTORY)) {
                    String macStr = fields[HistMacEnum.MAC.ordinal()];
                    String outPutValueStr = WeekMACJoinHisMR.HISTORY
                            + fields[HistMacEnum.QUDAO.ordinal()]
                            + DateFormatInfo.SEPARATOR
                            + fields[HistMacEnum.PLAT.ordinal()];
                    context.write(new Text(macStr), new Text(outPutValueStr));
                }
                else {
                    String macStr = fields[WeekMacEnum.MAC.ordinal()];
                    context.write(new Text(macStr), value);
                }
            }
        }

    }

    public static class WeekMACJoinHisReducer extends
            Reducer<Text, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(WeekMACJoinHisReducer.class.getName());

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            StringBuilder historyValueSB = new StringBuilder();
            StringBuilder thisWeekInfoSB = new StringBuilder();
            for (Text value : values) {
                String valueStr = value.toString();
                // System.out.println(valueStr);
                if (valueStr.contains(WeekMACJoinHisMR.HISTORY)) {
                    historyValueSB.append(valueStr
                            .substring(WeekMACJoinHisMR.HISTORY.length()));
                }
                else {

                    thisWeekInfoSB.append(valueStr);
                }
            }
            String historyValueStr = historyValueSB.toString().trim();
            String thisWeekInfoStr = thisWeekInfoSB.toString().trim();
            if (!"".equalsIgnoreCase(historyValueStr)
                    && !"".equalsIgnoreCase(thisWeekInfoStr)) {
                String[] thisWeekInfoFields = thisWeekInfoStr
                        .split(DateFormatInfo.SEPARATOR);

                String outkeyStr = historyValueStr + DateFormatInfo.SEPARATOR
                        + key.toString() + DateFormatInfo.SEPARATOR
                        + thisWeekInfoFields[WeekMacEnum.LOGINDAYS.ordinal()];
                context.write(
                        new Text(thisWeekInfoFields[WeekMacEnum.DATE_ID
                                .ordinal()]), new Text(outkeyStr));
            }
            if ("".equalsIgnoreCase(historyValueStr)
                    && !"".equalsIgnoreCase(thisWeekInfoStr)) {
                String[] thisWeekInfoFields = thisWeekInfoStr
                        .split(DateFormatInfo.SEPARATOR);
                String outkeyStr = thisWeekInfoFields[WeekMacEnum.QUDAO
                        .ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + thisWeekInfoFields[WeekMacEnum.PLAT.ordinal()]
                        + DateFormatInfo.SEPARATOR
                        + key.toString()
                        + DateFormatInfo.SEPARATOR
                        + thisWeekInfoFields[WeekMacEnum.LOGINDAYS.ordinal()];
                context.write(
                        new Text(thisWeekInfoFields[WeekMacEnum.DATE_ID
                                .ordinal()]), new Text(outkeyStr));
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
        int nRet = 0;
        try {
            commArgs.init("weekmacjoinhismr.jar");
            commArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // userNewOrOldCountArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new WeekMACJoinHisMR(),
                commArgs.getCommsParam());
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
        Job job = new Job(conf, "2_coredate_weekmacjoinhis");
        job.setJarByClass(WeekMACJoinHisMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WeekMACJoinHisMapper.class);
        job.setReducerClass(WeekMACJoinHisReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(24);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
