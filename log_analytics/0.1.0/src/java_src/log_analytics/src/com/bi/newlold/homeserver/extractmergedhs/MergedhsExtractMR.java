package com.bi.newlold.homeserver.extractmergedhs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MergedhsExtractMR {

    public enum HomeServerEnum {
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
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */

    public static class MergehdsExtractMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static Logger logger = Logger
                .getLogger(MergehdsExtractMapper.class.getName());

        public static final String SEPARATOR = "\t";

        public String month;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path path = fileSplit.getPath();
            month = getMonth(path.toString());

        }

        private String getMonth(String filePath) {
            StringBuilder path = new StringBuilder();
            String regEx = "(\\d+\\/\\d+)";
            Pattern pat = Pattern.compile(regEx);
            Matcher mat = pat.matcher(filePath);
            while (mat.find()) {
                for (int i = 1; i <= mat.groupCount(); i++) {
                    path.append(mat.group(i));
                }
            }
            path.append("/01");
            return path.toString().replace("/", "");

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder output = new StringBuilder();
            output.append(month);
            output.append(SEPARATOR);
            output.append(fields[HomeServerEnum.MACCODE.ordinal()]);
            output.append(SEPARATOR);
            output.append(fields[HomeServerEnum.TOTALDAY.ordinal()]);
            String outKey = output.toString();
            String outValue = "";
            context.write(new Text(outKey), new Text(outValue));
        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(MergedhsExtractMR.class);
        job.setJobName("MergedhsExtractMR-NEWOLDUSER");
        job.setMapperClass(MergehdsExtractMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("201305"));
        FileOutputFormat.setOutputPath(job, new Path("output_201305"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
