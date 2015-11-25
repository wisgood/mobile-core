package com.bi.dingzi;

/**
 * indextype = 0  钉子覆盖有客户端用户中IOS正式版数： 
   计算逻辑Fsplatform上报fsclient字段为1的guid与iphonecheck上报字段bro为2的guid的交集
   indextype = 1 钉子覆盖无客户端用户中IOS正式版数： 
   计算逻辑Fsplatform上报fsclient字段为0的guid与iphonecheck上报字段bro为2的guid的交集

 * 
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DingZiMR {

    public static class DingZiMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr;

        private int fileSource;

        private final int GUID_INDEX_FSPLATFORM = FsplatformEnum.GUID.ordinal();

        private final int GUID_INDEX_IPHONECHECK = IphoneCheckEnum.GUID
                .ordinal();

        private int indexType = 0;

        public void setup(Context context) throws NumberFormatException {

            try {
                indexType = Integer.parseInt(context.getConfiguration().get(
                        "indextype"));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            filePathStr = fileInputSplit.getPath().toUri().getPath();
            if (filePathStr.toLowerCase().contains("fsplatform".toLowerCase()))
                fileSource = 1;
            else {
                fileSource = 0;
            }

        }

        private boolean logLengthOk(int length) {
            if (fileSource == 1) {
                if (length < FsplatformEnum.FSCLIENT.ordinal() + 1)
                    return false;
            }
            else {
                if (length < IphoneCheckEnum.GUID.ordinal() + 1)
                    return false;

            }
            return true;

        }

        private boolean filterLog(int indexType, String[] fields) {
            if (indexType == 0) {

                if (fileSource == 1) {
                    int fsclient = Integer
                            .parseInt(fields[FsplatformEnum.FSCLIENT.ordinal()]);
                    if (fsclient != 1)
                        return true;
                }
                else {
                    int bro = Integer.parseInt(fields[IphoneCheckEnum.BRO
                            .ordinal()]);
                    if (bro != 2)
                        return true;

                }

            }
            else {
                if (fileSource == 1) {
                    int fsclient = Integer
                            .parseInt(fields[FsplatformEnum.FSCLIENT.ordinal()]);
                    if (fsclient != 0)
                        return true;
                }
                else {
                    int bro = Integer.parseInt(fields[IphoneCheckEnum.BRO
                            .ordinal()]);
                    if (bro != 2)
                        return true;

                }

            }

            return false;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String originLog = value.toString();
            String[] fields = originLog.split(",");
            StringBuilder outputValue = new StringBuilder();
            String outputKey;
            if (!logLengthOk(fields.length))
                return;
            try {
                if (filterLog(indexType, fields))
                    return;

            }
            catch(Exception e) {
                // TODO: handle exception
                return;
            }

            if (fileSource == 1) {
                outputKey = fields[GUID_INDEX_FSPLATFORM];

                outputValue.append(outputKey);
                outputValue.append("\t");
                outputValue.append(1);// record from fsplatform
                outputValue.append("\t");
                outputValue.append(0);// record not from fsplatform
            }
            else {
                outputKey = fields[GUID_INDEX_IPHONECHECK];
                outputValue.append(outputKey);
                outputValue.append("\t");
                outputValue.append(0);// record not from fsplatform
                outputValue.append("\t");
                outputValue.append(1);// record from fsplatform

            }
            context.write(new Text(outputKey), new Text(outputValue.toString()));

        }
    }

    public static class DingZiReducer extends Reducer<Text, Text, Text, Text> {
        private int sum;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum_fsplatform = 0;
            int sum_iphonecheck = 0;
            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                sum_fsplatform += Integer.parseInt(fields[1]);
                sum_iphonecheck += Integer.parseInt(fields[2]);

            }
            if (sum_fsplatform > 0 && sum_iphonecheck > 0)
                sum++;

        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            context.write(new Text(String.valueOf(sum)), new Text());
        }

    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Job job = new Job();
        job.setJarByClass(DingZiMR.class);
        job.setJobName("DingZiMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input_dingzi"));
        FileOutputFormat.setOutputPath(job, new Path("output_dingzi"));
        job.setMapperClass(DingZiMapper.class);
        job.setReducerClass(DingZiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
