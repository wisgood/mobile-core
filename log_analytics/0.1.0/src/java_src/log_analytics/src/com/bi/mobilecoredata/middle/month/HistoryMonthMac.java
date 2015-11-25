package com.bi.mobilecoredata.middle.month;

/**
 * 设姑娘成当
 */
import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class HistoryMonthMac extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    enum MonthMac {
        MONTH, QUDAO, PLAT, MACCODE
    }

    public static class HistoryMonthMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder outputKey = new StringBuilder();
            outputKey.append(fields[MonthMac.QUDAO.ordinal()]);
            outputKey.append(SEPARATOR);
            outputKey.append(fields[MonthMac.PLAT.ordinal()]);
            outputKey.append(SEPARATOR);
            try {
                outputKey.append(fields[MonthMac.MACCODE.ordinal()]);
            }
            catch(Exception e) {
                    System.out.println(value.toString());
                    return ;
            }
            context.write(new Text(outputKey.toString()), new Text(
                    fields[MonthMac.MONTH.ordinal()]));
        }
    }

    public static class HistoryMonthMacReducer extends
            Reducer<Text, Text, Text, Text> {

        private int currentMonth;

        private int lastMonth;

        private int previousMonth;

        private int currentMonthTag;

        private int lastMonthTag;

        private int previousMonthTag;

        // private int totalDay;

        public void setup(Context context) throws NumberFormatException {

            try {
                currentMonth = Integer.parseInt(context.getConfiguration().get(
                        "current"));
                lastMonth = Integer.parseInt(context.getConfiguration().get(
                        "last"));
                previousMonth = Integer.parseInt(context.getConfiguration()
                        .get("previous"));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            currentMonthTag = 0;
            lastMonthTag = 0;
            previousMonthTag = 0;

            for (Text value : values) {
                int month = Integer.parseInt(value.toString());
                if (month == currentMonth) {
                    currentMonthTag = 1;
                }
                else if (month == lastMonth) {
                    lastMonthTag = 1;
                }
                else if (month == previousMonth) {
                    previousMonthTag = 1;
                }

            }
            StringBuilder output = new StringBuilder();
            output.append(currentMonth + SEPARATOR);
            output.append(key.toString() + SEPARATOR);
            output.append(currentMonthTag + SEPARATOR);
            output.append(lastMonthTag + SEPARATOR);
            output.append(previousMonthTag + SEPARATOR);
            context.write(new Text(output.toString()), new Text(""));
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

        nRet = ToolRunner.run(new Configuration(), new HistoryMonthMac(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        String currentMonthInputPath = args[0];
        conf.set("current", getFirstDayOfMonth(currentMonthInputPath));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parseDate(getFirstDayOfMonth(currentMonthInputPath)));
        calendar.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
        conf.set("last", dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
        conf.set("previous", dateFormat.format(calendar.getTime()));
        Job job = new Job(conf, "2_coredata_history_month_mac");
        job.setJarByClass(HistoryMonthMac.class);
        for (String path : getPreThreeMonthInputPaths(currentMonthInputPath)) {
            if (fileExist(path)) {
                FileInputFormat.addInputPath(job, new Path(path));
            }

        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(HistoryMonthMacMapper.class);
        job.setReducerClass(HistoryMonthMacReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    private String[] getPreThreeMonthInputPaths(String currentMonthInputPath) {
        List<String> paths = new LinkedList<String>();
        paths.add(currentMonthInputPath);
        int length = currentMonthInputPath.length();
        String pathPrefix = currentMonthInputPath.substring(0, length - 8);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(parseDate(getFirstDayOfMonth(currentMonthInputPath)));
        calendar.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
        SimpleDateFormat dateFormat = new SimpleDateFormat("/yyyy/MM");
        paths.add(pathPrefix + dateFormat.format(calendar.getTime()));
        calendar.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
        paths.add(pathPrefix + dateFormat.format(calendar.getTime()));
        return paths.toArray(new String[paths.size()]);

    }

    private String getFirstDayOfMonth(String filePath) {
        int endIndex = filePath.length();
        String month = filePath.substring(endIndex - 2, endIndex);
        String year = filePath.substring(endIndex - 7, endIndex - 3);
        String day = "01";
        return year + month + day;

    }

    private Date parseDate(String dateString) {
        Date date = null;
        try {
            date = dateFormat.parse(dateString);
        }
        catch(ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return date;
    }

    private boolean fileExist(String path) {
        String uri = path;
        Configuration conf = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(URI.create(uri), conf);
            return fileSystem.getFileStatus(new Path(uri)) != null;
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        return false;

    }

    static class ParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            return "";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            return "";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

}
