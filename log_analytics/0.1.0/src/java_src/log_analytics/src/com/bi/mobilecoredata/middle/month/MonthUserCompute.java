package com.bi.mobilecoredata.middle.month;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class MonthUserCompute extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

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

        nRet = ToolRunner.run(new Configuration(), new MonthUserCompute(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_month_user");
        job.setJarByClass(MonthUserCompute.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MonthUserComputeMapper.class);
        job.setReducerClass(MonthUserComputeReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    static class ParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "extract the field from the  log";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar fbufferformat.jar ";
            return "Usage :  \n    " + functionUsage + "\n";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

    public enum HistoryMonthMac {
        DATE, QUDAO, PLAT, MACCODE, CURRENT, LAST, PREVIOUS
    }

    public static class MonthUserComputeMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            String date = fields[HistoryMonthMac.DATE.ordinal()];
            String qudao = fields[HistoryMonthMac.QUDAO.ordinal()];
            String plat = fields[HistoryMonthMac.PLAT.ordinal()];
            String mac = fields[HistoryMonthMac.MACCODE.ordinal()];
            String currentTag = fields[HistoryMonthMac.CURRENT.ordinal()];
            String lastTag = fields[HistoryMonthMac.LAST.ordinal()];
            String previousTag = fields[HistoryMonthMac.PREVIOUS.ordinal()];

            context.write(new Text(qudao + SEPARATOR + plat), new Text(mac
                    + SEPARATOR + date + SEPARATOR + currentTag + SEPARATOR
                    + lastTag + SEPARATOR + previousTag));

        }

    }

    public static class MonthUserComputeReducer extends
            Reducer<Text, Text, Text, Text> {

        private static final String EXSIT = "1";

        private String date;

        private long currentMonthTotalUserSum;

        private long currentMonthNewUserSum;

        private long currentMonthOldUserSum;

        private long lastMonthOldUserSum;

        private long lastMonthNewUserSum;

        private long lastMonthLostUserSum;

        private long lastMonthTotalUserSum;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            currentMonthTotalUserSum = 0;

            currentMonthNewUserSum = 0;

            currentMonthOldUserSum = 0;

            lastMonthOldUserSum = 0;

            lastMonthNewUserSum = 0;

            lastMonthLostUserSum = 0;

            lastMonthTotalUserSum = 0;
            for (Text value : values) {
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);
                date = fields[1];
                String currentTag = fields[2];
                String lastTag = fields[3];
                String previousTag = fields[4];
                if (EXSIT.equals(currentTag)) {
                    currentMonthTotalUserSum++;
                    if (EXSIT.equals(lastTag)) {
                        currentMonthOldUserSum++;
                        lastMonthTotalUserSum++;
                        if (EXSIT.equals(previousTag)) {
                            lastMonthOldUserSum++;
                        }
                        else {
                            lastMonthNewUserSum++;

                        }
                    }
                    else {
                        currentMonthNewUserSum++;
                    }
                }
                else {
                    if (EXSIT.equals(lastTag)) {
                        lastMonthLostUserSum++;
                        lastMonthTotalUserSum++;
                    }

                }
            }

            context.write(new Text(date + SEPARATOR + key.toString()),
                    new Text(currentMonthTotalUserSum + SEPARATOR
                            + currentMonthNewUserSum + SEPARATOR
                            + currentMonthOldUserSum + SEPARATOR
                            + lastMonthOldUserSum + SEPARATOR
                            + lastMonthNewUserSum + SEPARATOR
                            + lastMonthTotalUserSum + SEPARATOR
                            + lastMonthLostUserSum));

        }
    }

}
