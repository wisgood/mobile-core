package com.bi.mobilecoredata.middle.day;

/**
 * 
 * 移动核心数据2期：日启动用户数 日新增用户数 日观看时长
 * 输入日志为3个，分别为
 * date_qudao_plat_bootstrap_distintbymaccod_count
 * date_qudao_plat_new_user_count
 * date_qudao_plat_playtm_vtm_sum
 * 输出格式为 日期 渠道 平台 日启动用户数 日新增用户数 日观看时长
 * 
 * 
 *
 */
import jargs.gnu.CmdLineParser.Option;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class DayUserDetail extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    /**
     * 输入日志格式
     */
    private enum Log {
        DATE, CHANNEL, PLAT, SUM
    }

    public static class DayUserDetailMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Path path;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            path = fileSplit.getPath();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder outputkey = new StringBuilder();
            outputkey.append(fields[Log.DATE.ordinal()]);
            outputkey.append(SEPARATOR);
            outputkey.append(fields[Log.CHANNEL.ordinal()]);
            outputkey.append(SEPARATOR);
            outputkey.append(fields[Log.PLAT.ordinal()]);

            StringBuilder outputValue = new StringBuilder();
            if (path.toString().contains("bootstrap")) {
                outputValue.append(fields[Log.SUM.ordinal()]);
                outputValue.append(SEPARATOR);
                outputValue.append("0");
                outputValue.append(SEPARATOR);
                outputValue.append("0");
            }
            else if (path.toString().contains("new_user")) {
                outputValue.append("0");
                outputValue.append(SEPARATOR);
                outputValue.append(fields[Log.SUM.ordinal()]);
                outputValue.append(SEPARATOR);
                outputValue.append("0");
            }
            else {
                outputValue.append("0");
                outputValue.append(SEPARATOR);
                outputValue.append("0");
                outputValue.append(SEPARATOR);
                outputValue.append(fields[Log.SUM.ordinal()]);
            }

            context.write(new Text(outputkey.toString()),
                    new Text(outputValue.toString()));
        }

    }

    public static class DayUserDetailReducer extends
            Reducer<Text, Text, Text, Text> {
        private long dayBootUserSum;

        private long dayNewUserSum;

        private long dayPlaytmSum;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            dayBootUserSum = 0;

            dayNewUserSum = 0;

            dayPlaytmSum = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                dayBootUserSum += Long.parseLong(fields[0]);
                dayNewUserSum += Long.parseLong(fields[1]);
                dayPlaytmSum += Long.parseLong(fields[2]);

            }
            if (dayBootUserSum == 0 && dayNewUserSum == 0 && dayPlaytmSum == 0)
                return;
            context.write(key, new Text(dayBootUserSum + SEPARATOR
                    + dayNewUserSum + SEPARATOR + dayPlaytmSum));

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new DayUserDetail(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    private static class ParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            return "";
        }

        @Override
        public String getFunctionUsage() {
            return "";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_dayuserdetail");
        job.setJarByClass(DayUserDetail.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DayUserDetailMapper.class);
        job.setReducerClass(DayUserDetailReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
