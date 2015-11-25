package com.bi.mobilecoredata.middle.month;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class MonthMac extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    /**
     * the input log form
     */
    private enum Log {
        DATE, HOUR, PLAT, VERSION, QUDAO, CITY, MAC, TIMESTAMP
    }

    public static class MonthMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String firstDayOfMonth;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path path = fileSplit.getPath().getParent();
            firstDayOfMonth = getFirstDayOfMonth(path.toString());

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            String outputKey = firstDayOfMonth+SEPARATOR+
                    fields[Log.QUDAO.ordinal()] + SEPARATOR
                    + fields[Log.PLAT.ordinal()] ;
            context.write(new Text(fields[Log.MAC.ordinal()]), new Text(outputKey));
        }
    }

    private static String getFirstDayOfMonth(String dateId) {
        int endIndex = dateId.length();
        String month = dateId.substring(endIndex - 5, endIndex - 3);
        String year = dateId.substring(endIndex - 10, endIndex - 6);
        String day = "01";
        return year + month + day;

    }

    public static class MonthMacReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(new Text(value.toString()),
                        new Text(key.toString()));
                break;
            }

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

        nRet = ToolRunner.run(new Configuration(), new MonthMac(),
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
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_monthmac");
        job.setJarByClass(MonthMac.class);
        for (String path : getMonthInputPaths(args[0])) {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MonthMacMapper.class);
        job.setReducerClass(MonthMacReducer.class);
        job.setNumReduceTasks(30);
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

}
