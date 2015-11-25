package com.bi.newlold.homeserver.mergedhs.format;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MergedhsExtractMR {

    public enum MergedhsEnum {
        /**
         * 
         * MAC地址， 初次登录渠道， 初次登录时间， 初次登录版本， 初次登录IP 末次登录渠道， 末次登录时间， 末次登录版本， 末次登录IP
         * 末次下线渠道， 末次下线时间， 末次下线版本， 末次下线IP 总登录次数， 总登录天数， 总登录时长， 总上传流量， 总下载流量
         * 
         * 
         */
        MACCODE, CHUQDAO, CHULOGIN, CHUVER, CHUIP, LASTQUDAO, LASTTIME, LASTVER, LASTIP, LASTDQUDAO, LASTDTIME, LASTDVER, LASTDIP, TOATLLOGIN, TOTALDAY, TOTALTIME, TOTALDLIULIANG, TOTALULIULIANG

    }

    /**
     * 
     * @Title: main
     * @Description: extract the needed field of month log
     * @param @param args
     * @return void
     * @throws
     */

    public static class MergehdsExtractMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public static final String SEPARATOR = "\t";

        public String firstDayOfMonth;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path path = fileSplit.getPath().getParent();
            firstDayOfMonth = getFirstDayOfMonth(path.toString());

        }

        private String getFirstDayOfMonth(String filePath) {
            int endIndex = filePath.length();
            String month = filePath.substring(endIndex - 2, endIndex);
            String year = filePath.substring(endIndex - 7, endIndex - 3);
            String day = "01";
            return year + month + day;

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder output = new StringBuilder();
            output.append(firstDayOfMonth);
            output.append(SEPARATOR);
            output.append(fields[MergedhsEnum.MACCODE.ordinal()]);
            output.append(SEPARATOR);
            output.append(fields[MergedhsEnum.TOTALDAY.ordinal()]);
            String outKey = output.toString();
            String outValue = "";
            context.write(new Text(outKey), new Text(outValue));
        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Job job = new Job();
        job.setJarByClass(MergedhsExtractMR.class);
        job.setJobName("MergedhsExtractMR-newolduser");
        job.setMapperClass(MergehdsExtractMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("201305"));
        FileOutputFormat.setOutputPath(job, new Path("output_201305"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
