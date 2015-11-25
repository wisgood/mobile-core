package com.bi.format.pushreach;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.logenum.FormatPushreachEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.MidUtil;

public class Pushreach2HourExtract extends Configured implements Tool {

    public static class Pushreach2HourExtractMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private MidUtil midUtil;

        private static final int PLAT_INDEX = 2;

        private static long timespace = 2 * 60 * 60 * 1000;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            String midPath = context.getConfiguration().get("mid");
            midUtil = MidUtil.getInstance(midPath);

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = DataFormatUtils.split(line,
                    DataFormatUtils.TAB_SEPARATOR, 0);
            int plat = Integer.parseInt(fields[PLAT_INDEX]);
            boolean ios = plat == 3 || plat == 4;
            if (ios)
                return;

            long timestamp = Long
                    .parseLong(fields[FormatPushreachEnum.TIMESTAMP.ordinal()]);
            boolean timeOk = (0 == timespace)
                    || midUtil.containInTimeSpace(plat, timestamp,
                            CommonConstant.TOWHOUR);
            if (!timeOk) {
                return;
            }

            context.write(new Text(value.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new BaseCmdParamParse() {
            @Override
            public Option[] getOptions() {
                List<Option> options = new ArrayList<Option>(0);
                Option midOption = getParser()
                        .addHelp(getParser().addStringOption("mid"),
                                "the mid file path");
                options.add(midOption);

                return options.toArray(new Option[options.size()]);

            }

            @Override
            public String[] getParams() {
                // TODO Auto-generated method stub

                String[] params = super.getParams();
                int length = params.length;
                // file param must be the first
                String[] convertParam = new String[length + 2];
                System.arraycopy(params, 0, convertParam, 2, length);
                convertParam[0] = "-files";
                convertParam[1] = params[length - 1];
                return convertParam;
            }
        };
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new Pushreach2HourExtract(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("mid", args[2]);
        Job job = new Job(conf, "pushreach-pushreach-2hour-extract");
        job.setJarByClass(Pushreach2HourExtract.class);
        HdfsUtil.deleteDir(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Pushreach2HourExtractMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}