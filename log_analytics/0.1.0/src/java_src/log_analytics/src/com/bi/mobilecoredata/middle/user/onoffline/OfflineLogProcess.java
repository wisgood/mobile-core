package com.bi.mobilecoredata.middle.user.onoffline;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: OfflineLogProcess.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 离线日志处理 根据日志日期，将日志日期之后七天的日志作为输出
 * 
 * @ouputFormat:DateId MacCode ..
 */
import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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

import com.bi.comm.paramparse.AbstractCommandParamParse;
import com.bi.comm.util.DateUtil;
import com.bi.comm.util.HdfsUtil;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class OfflineLogProcess extends Configured implements Tool {

    private enum BootStrap {
        TIMESTAMP, IP, DEV, MAC, VER, NT, BTYPE, BTIME, OK, SR, MEM, TDISK, FDISK, SID, RT, IPHONEIP, BROKEN, IMEI, INSTALLT, FUDID;
    }

    private enum Exit {
        TIMESTAMP, IP, DEV, MAC, VER, NT, USETM, TN, SID, RT, IPHONEIP, FUDID;
    }

    public static class GenerateDayOfflineLogMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private Path filePath;

        private long dateBegin;

        private long dateEnd;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent();
            String date = context.getConfiguration().get("date");
            dateBegin = getDayBeginAndEnd(date, 0);
            dateEnd = getDayBeginAndEnd(date, 1);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");
            if (!lengthOk(fields.length, filePath.toString()))
                return;

            // data
            String dateInfo;
            if (filePath.toString().contains("bootstrap")) {
                dateInfo = fields[BootStrap.RT.ordinal()];
            }
            else {
                dateInfo = fields[Exit.RT.ordinal()];
            }
            long dateValue = 0L;
            try {
                dateValue = Long.parseLong(dateInfo);
            }
            catch(Exception e) {
                // TODO: handle exception
                return;
            }
            if (!dateOk(dateValue)) {
                return;
            }
            fields[0] = String.valueOf(dateBegin);
            StringBuilder outputBuilder = new StringBuilder();
            for (String string : fields)
                outputBuilder.append(string + "\t");
            context.write(new Text(outputBuilder.toString()), new Text(""));

        }

        private boolean lengthOk(int length, String fileName) {
            if (fileName.contains("bootstrap"))
                return length <= 20 && length >= 15;
            else {
                return length <= 12 && length >= 10;

            }
        }

        private boolean dateOk(long date) {
            return date <= dateEnd && date >= dateBegin;
        }

        private long getDayBeginAndEnd(String date, int tag) {
            Calendar currentDate = new GregorianCalendar();
            currentDate.setTime(DateUtil.StringToDate(date));

            if (tag == 0) {
                currentDate.set(Calendar.HOUR_OF_DAY, 0);
                currentDate.set(Calendar.MINUTE, 0);
                currentDate.set(Calendar.SECOND, 0);
            }
            else {
                currentDate.set(Calendar.HOUR_OF_DAY, 23);
                currentDate.set(Calendar.MINUTE, 59);
                currentDate.set(Calendar.SECOND, 59);
            }
            return currentDate.getTime().getTime() / 1000;
        }

    }

    public static class GenerateDayOfflineLogReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new OfflineLogProcess(),
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
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_offlinelog");
        job.setJarByClass(OfflineLogProcess.class);
        for (String path : getNextWeekPath(args[0])) {
            try {
                FileInputFormat.addInputPath(job, new Path(path));
            }
            catch(Exception e) {
            }
        }
        job.getConfiguration().set("date", getDate(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(GenerateDayOfflineLogMapper.class);
        job.setReducerClass(GenerateDayOfflineLogReducer.class);
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(15);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;

    }

    private static String[] getNextWeekPath(String currentMonthInputPath) {
        List<String> paths = new LinkedList<String>();
        paths.add(currentMonthInputPath);
        int length = currentMonthInputPath.length();
        String pathPrefix = currentMonthInputPath.substring(0, length - 10);
        String dateInfo = currentMonthInputPath.substring(length - 10, length);
        Date date = DateUtil.StringToDate(dateInfo);
        for (int i = 1; i <= 6; i++) {
            date = DateUtil.addDay(date, 1);
            dateInfo = DateUtil.DateToString(date, "yyyy/MM/dd");
            paths.add(pathPrefix + dateInfo);
        }

        return paths.toArray(new String[paths.size()]);

    }

    private static String getDate(String path) {
        int length = path.length();
        return path.substring(length - 10, length);
    }

}