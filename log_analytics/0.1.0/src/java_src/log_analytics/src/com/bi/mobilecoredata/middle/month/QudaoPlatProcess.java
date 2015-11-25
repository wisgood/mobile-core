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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.paramparse.AbstractCommandParamParse;

public class QudaoPlatProcess extends Configured implements Tool {
    private static final String SEPARATOR = "\t";

    private enum MonthMacLog {
        DATE, QUDAO, PLAT, MAC
    }

    private enum HistoryMacLog {
        MAC, QUDAO, PLAT
    }

    public static class QudaoPlatProcessMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private Path filePath;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);
                String outputKey = null;
                String outputValue = null;
                if (fromHistory(filePath.toString())) {
                    outputKey = fields[HistoryMacLog.MAC.ordinal()];
                    outputValue = "0" + SEPARATOR
                            + fields[HistoryMacLog.QUDAO.ordinal()] + SEPARATOR
                            + fields[HistoryMacLog.PLAT.ordinal()];
                }
                else {
                    outputKey = fields[MonthMacLog.MAC.ordinal()];
                    outputValue = "1" + SEPARATOR
                            + fields[MonthMacLog.DATE.ordinal()] + SEPARATOR
                            + fields[MonthMacLog.QUDAO.ordinal()] + SEPARATOR
                            + fields[MonthMacLog.PLAT.ordinal()];
                }

                context.write(new Text(outputKey), new Text(outputValue));
            }
            catch(Exception e) {
            }

        }

        private boolean fromHistory(String filePath) {
            return filePath.contains("history");
        }
    }

    public static class QudaoPlatProcessReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String qudao = null;
            String plat = null;
            String date = null;
            boolean output = false;

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                if ("0".equals(fields[0])) {
                    qudao = fields[1];
                    plat = fields[2];
                }
                else {
                    output = true;
                    date = fields[1];
                    qudao = fields[2];
                    plat = fields[3];
                }

            }
            if (output) {
                context.write(new Text(date + SEPARATOR + qudao + SEPARATOR
                        + plat), new Text(key.toString()));
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

        nRet = ToolRunner.run(new Configuration(), new QudaoPlatProcess(),
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
        Job job = new Job(conf, "2_coredate_monthmac_platprocess");
        job.setJarByClass(QudaoPlatProcess.class);
        for (String path : args[0].split(",")) {
            try {
                FileInputFormat.addInputPath(job, new Path(path));
            }
            catch(IOException e) {
            }

        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(QudaoPlatProcessMapper.class);
        job.setReducerClass(QudaoPlatProcessReducer.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
