package com.bi.mobilecoredata.middle.month;

/**
 * 生成一个月的启动用户 新增用户 播放时长
 * 输入为一个月的日明细
 * 
 * 
 */
import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.LinkedList;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class MonthDetail extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    /**
     * the input log form
     */
    private enum Log {
        DATE, CHANNEL, PLAT, BOOTUSERSUM, NEWUSERSUM, PLAYTMSUM
    }

    public static class MonthUserDetailMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            String outputKey = fields[Log.CHANNEL.ordinal()] + SEPARATOR
                    + fields[Log.PLAT.ordinal()];
            String outputValue = fields[Log.DATE.ordinal()] + SEPARATOR
                    + fields[Log.BOOTUSERSUM.ordinal()] + SEPARATOR
                    + fields[Log.NEWUSERSUM.ordinal()] + SEPARATOR
                    + fields[Log.PLAYTMSUM.ordinal()];
            context.write(new Text(outputKey), new Text(outputValue));
        }

    }

    public static class MonthUserDetailReducer extends
            Reducer<Text, Text, Text, Text> {
        private long monthBootUserSum;

        private long monthNewUserSum;

        private long monthPlaytmSum;

        private int daysOfMonth;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String firstDayOfMonth = null;
            monthBootUserSum = 0;

            monthNewUserSum = 0;

            monthPlaytmSum = 0;

            daysOfMonth = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                firstDayOfMonth = getFirstDayOfMonth(fields[0]);
                monthBootUserSum += Long.parseLong(fields[1]);
                monthNewUserSum += Long.parseLong(fields[2]);
                monthPlaytmSum += Long.parseLong(fields[3]);
                daysOfMonth = getDaysOfMonth(firstDayOfMonth);

            }
//            double avgDayBootUser = monthBootUserSum / daysOfMonth;
//            double avgDayNewUser = monthNewUserSum / daysOfMonth;
            String outputKey = firstDayOfMonth + SEPARATOR + key.toString();
            String outputValue = monthBootUserSum + SEPARATOR + monthNewUserSum
                    + SEPARATOR + monthPlaytmSum + SEPARATOR + daysOfMonth;

            context.write(new Text(outputKey), new Text(outputValue));

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

        nRet = ToolRunner.run(new Configuration(), new MonthDetail(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    static class ParamParse extends AbstractCommandParamParse {

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
            return new Option[0];
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_monthuserdetail");
        job.setJarByClass(MonthDetail.class);
        for (String path : getMonthInputPaths(args[0])) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

//         FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MonthUserDetailMapper.class);
        job.setReducerClass(MonthUserDetailReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    private static String[] getMonthInputPaths(String currentMonthInputPath) {

        int length = currentMonthInputPath.length();
        String pathPrefix = currentMonthInputPath.substring(0, length - 11);
        String dateId = currentMonthInputPath.substring(length - 10, length);
        int endIndex = dateId.length() - 1;
        int month = Integer.parseInt(dateId.substring(endIndex - 4,
                endIndex - 2));
        int year = Integer.parseInt(dateId
                .substring(endIndex - 9, endIndex - 5));
        DateTime dateTime = new DateTime(year, month, 1, 0, 0, 0, 0);

        int begin = dateTime.getDayOfMonth();
        int end = dateTime.dayOfMonth().getMaximumValue();
        List<String> paths = new LinkedList<String>();
        for (; begin <= end; begin++) {
            paths.add(pathPrefix + dateTime.toString("/yyyy/MM/dd"));
            dateTime = dateTime.plusDays(1);
        }

        return paths.toArray(new String[paths.size()]);

    }

    private static String getFirstDayOfMonth(String dateId) {
        int endIndex = dateId.length();
        String month = dateId.substring(endIndex - 4, endIndex - 2);
        String year = dateId.substring(endIndex - 8, endIndex - 4);
        String day = "01";
        return year + month + day;

    }

    private static int getDaysOfMonth(String dateId) {
        int endIndex = dateId.length();
        int month = Integer.parseInt(dateId.substring(endIndex - 4,
                endIndex - 2));
        int year = Integer.parseInt(dateId
                .substring(endIndex - 8, endIndex - 4));
        DateTime dateTime = new DateTime(year, month, 1, 0, 0, 0, 0);
        return dateTime.dayOfMonth().getMaximumValue();

    }
}