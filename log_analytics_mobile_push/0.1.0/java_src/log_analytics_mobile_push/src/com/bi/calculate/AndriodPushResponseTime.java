/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: AndriodPushResponseTime.java 
 * @Package com.bi.calculate 
 * @Description: 安桌推送响应时间
 * @author wang
 * @date 2013-10-8 上午12:45:34 
 * @input:输入日志路径/2013-10-8
 * @output:输出日志路径/2013-10-8
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:
 */
package com.bi.calculate;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.logenum.FormatPushreachEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

/**
 * @ClassName: AndriodPushResponseTime
 * @Description: 这里用一句话描述这个类的作用
 * @author wang
 * @date 2013-10-8 上午12:45:34
 */
public class AndriodPushResponseTime extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodPushResponseTimeMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePath;

        private static final int PLAT_INDEX = 2;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtil.splitLog(line, SEPERATOR);
            int plat = Integer.parseInt(fields[PLAT_INDEX]);
            boolean ios = (plat == 3) || (plat == 4);
            if (ios)
                return;
            int fileTag = 0;
            String mac = "";
            String timeStamp = "";
            String date = "";
            String hour = "";
            if (fromBootstrap(filePath)) {

                int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                        .ordinal()]);
                boolean desktop = (btype == 7);
                boolean notice = (btype == 3);
                if (!desktop && !notice)
                    return;
                mac = fields[FormatBootStrapEnum.MAC.ordinal()];
                date = fields[FormatBootStrapEnum.DATE_ID.ordinal()];
                hour = fields[FormatBootStrapEnum.HOUR_ID.ordinal()];

                fileTag = 1;
                timeStamp = fields[FormatBootStrapEnum.TIMESTAMP.ordinal()];

            }
            else if (fromPushreach(filePath)) {
                mac = fields[FormatPushreachEnum.MAC.ordinal()];
                date = fields[FormatPushreachEnum.DATE_ID.ordinal()];
                hour = fields[FormatPushreachEnum.HOUR_ID.ordinal()];
                fileTag = 2;
                timeStamp = fields[FormatPushreachEnum.TIMESTAMP.ordinal()];
                int ok = Integer.parseInt(fields[FormatPushreachEnum.OK
                        .ordinal()]);
                if (ok != 1)
                    return;

            }

            context.write(new Text(mac), new Text(fileTag + "\t" + timeStamp
                    + "\t" + date + "\t" + hour + "\t" + plat));

        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }

        private boolean fromPushreach(String filePath) {
            return filePath.toLowerCase().contains("pushreach".toLowerCase());
        }

    }

    public static class AndriodPushResponseTimeReducer extends
            Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean fromBootstrap = false;
            boolean fromPushreach = false;
            String date = "";
            String hour = "";
            String plat = "";

            Set<Long> boottimeSet = new TreeSet<Long>();
            Set<Long> pushreachtimeSet = new TreeSet<Long>();

            for (Text value : values) {

                String[] fields = StringUtil.splitLog(value.toString(), '\t');
                int fileTag = Integer.parseInt(fields[0]);
                date = fields[2];
                hour = fields[3];
                plat = fields[4];
                switch (fileTag) {
                case 1:
                    fromBootstrap = true;
                    boottimeSet.add(Long.parseLong(fields[1]));
                    break;
                case 2:
                    fromPushreach = true;
                    pushreachtimeSet.add(Long.parseLong(fields[1]));
                    break;
                default:
                    break;
                }

            }

            if (fromBootstrap && fromPushreach) {
                Long[] boottime = boottimeSet.toArray(new Long[boottimeSet
                        .size()]);
                Long[] pushreachtime = pushreachtimeSet
                        .toArray(new Long[pushreachtimeSet.size()]);
                long responseTimeSum = 0L;
                for (int i = 0; i < boottime.length; i++) {
                    for (int j = pushreachtime.length - 1; j >= 0; j--) {
                        long responsetime = (long) boottime[i]
                                - (long) pushreachtime[j];
                        if (responsetime > 0) {
                            responseTimeSum += responsetime;
                            break;
                        }

                    }
                }
                context.write(new Text(date + "\t" + hour + "\t" + plat),
                        new Text(boottime.length + "\t" + responseTimeSum));
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

        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(),
                new AndriodPushResponseTime(), paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "andriod-pushreach-response-time");
        job.setJarByClass(AndriodPushResponseTime.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodPushResponseTimeMapper.class);
        job.setReducerClass(AndriodPushResponseTimeReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
