/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: IOSWatchDownMac.java 
 * @Package com.bi.calculate 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-29 上午10:24:38 
 * @input:输入日志路径/2013-9-29
 * @output:输出日志路径/2013-9-29
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.calculate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import com.bi.common.logenum.DownloadEnum;
import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.logenum.FormatFbufferEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.MidUtil;
import com.bi.common.util.StringUtil;

/**
 * @ClassName: IOSWatchDownMac
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-29 上午10:24:38
 */
public class IOSWatchDownMacMR extends Configured implements Tool {

    public static class IOSWatchDownMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePath;

        private static final int PLAT_INDEX = 2;

        private MidUtil midUtil;

        private long timespace;

        private Text keyText;

        private Text valueText;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            this.keyText = new Text();
            this.valueText = new Text();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            String midPath = context.getConfiguration().get(
                    CommonConstant.MIDFILENAME);
            midUtil = MidUtil.getInstance(midPath);
            try {
                timespace = context.getConfiguration().getLong(
                        CommonConstant.TIMESPACE, 0);
                // System.out.println("timespace:"+timespace);
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
            boolean ios = (plat == 3) || (plat == 4);
            if (!ios)
                return;
            int fileTag = 0;
            String mac = "";
            if (fromBootstrap(filePath)) {
                int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                        .ordinal()]);
                boolean desktop = (btype == 3);
                boolean notice = (btype == 2);
                boolean isNoDesktopAndNotice = !desktop && !notice;
                if (isNoDesktopAndNotice) {
                    return;
                }
                long timestamp = Long
                        .parseLong(fields[FormatBootStrapEnum.TIMESTAMP
                                .ordinal()]);
                boolean isNOContainInTimeSpaceValues = (null != midUtil
                        && 0 != timespace && !midUtil.containInTimeSpace(plat,
                        timestamp, CommonConstant.TOWHOUR));
                if (isNOContainInTimeSpaceValues) {
                    return;
                }
                mac = fields[FormatBootStrapEnum.MAC.ordinal()];
                fileTag = 1;
            }
            else if (fromFbuffer(filePath)) {
                String mid = fields[FormatFbufferEnum.MID.ordinal()];
                long timestamp = Long
                        .parseLong(fields[FormatFbufferEnum.TIMESTAMP.ordinal()]);
                boolean isContainInTimeSpaceValues = (timespace > 0 ? midUtil
                        .containMid(plat, mid, timestamp, timespace) : midOk(
                        plat, mid));
                if (!isContainInTimeSpaceValues) {
                    return;
                }
                mac = fields[FormatFbufferEnum.MAC.ordinal()];
                fileTag = 2;
            }
            else if (fromDownload(filePath)) {
                String mid = fields[FormatFbufferEnum.MID.ordinal()];
                long timestamp = Long
                        .parseLong(fields[FormatFbufferEnum.TIMESTAMP.ordinal()]);
                boolean isContainInTimeSpaceValues = (timespace > 0 ? midUtil
                        .containMid(plat, mid, timestamp, timespace) : midOk(
                        plat, mid));
                if (!isContainInTimeSpaceValues) {
                    return;
                }
                mac = fields[DownloadEnum.MAC.ordinal()];
                fileTag = 3;
            }
            keyText.set(mac);
            valueText.set(fileTag + "" + DataFormatUtils.TAB_SEPARATOR
                    + value.toString());
            context.write(keyText, valueText);
        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }

        private boolean fromFbuffer(String filePath) {
            return filePath.toLowerCase().contains("fbuffer".toLowerCase());
        }

        private boolean fromDownload(String filePath) {
            return filePath.toLowerCase().contains("download".toLowerCase());
        }

        private boolean midOk(int plat, String mid) {
            return midUtil.containMid(plat, mid);

        }

    }

    public static class IOSWatchDownMacReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean fromBootstrap = false;
            int fbufferExsitTag = 0;
            int downloadExsitTag = 0;
            List<String> bootstrapValueList = new ArrayList<String>(30);
            for (Text value : values) {
//                System.out.println(value);
                int fileTag = Integer.parseInt(StringUtil.splitLog(
                        value.toString(), '\t')[0]);
                switch (fileTag) {
                case 1:
                    bootstrapValueList
                            .add(processOutputValue(value.toString()));
                    fromBootstrap = true;
                    break;
                case 2:
                    fbufferExsitTag = 1;
                    break;
                case 3:
                    downloadExsitTag = 1;
                    break;
                default:
                    break;
                }

            }

            if (fromBootstrap
                    && (fbufferExsitTag == 1 || downloadExsitTag == 1))
                for (int index = 0; index < bootstrapValueList.size(); index++) {
                    context.write(new Text(bootstrapValueList.get(index)),
                            new Text(fbufferExsitTag + "\t" + downloadExsitTag));

                }

        }

        private String processOutputValue(String origin) {
            String[] fields = StringUtil.splitLog(origin, '\t');
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < fields.length; i++) {
                sb.append(fields[i]);
                if (i != fields.length)
                    sb.append("\t");
            }
            return sb.toString();

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
        int nRet = ToolRunner.run(new Configuration(), new IOSWatchDownMacMR(),
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
        job.setJarByClass(IOSWatchDownMacMR.class);
        job.setMapperClass(IOSWatchDownMacMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IOSWatchDownMacReducer.class);
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
