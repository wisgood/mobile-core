package com.bi.result.playfailed;

/**
 * author :wangxw
 * 
 * 播放失败的计算程序 
 */

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

import com.bi.common.logenum.FormatFbufferEnum;
import com.bi.common.logenum.FormatPlayFailEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.KeyCombinedDimensionUtil;

public class PlayFailedCalculate extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    public static class PlayFailedCalculateMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Path filePath = null;

        private int[] groupByColumns = { 0, 2, 3, 4, 5, 7 };

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            StringBuilder outValue = new StringBuilder();
            if (fromAppFbuffer(filePath)) {
                outValue.append("0");
                outValue.append(SEPARATOR);
                outValue.append(fields[FormatFbufferEnum.MAC_FORMAT.ordinal()]);
                List<String> outKeyList = KeyCombinedDimensionUtil
                        .getOutputKey(fields, groupByColumns);
                for (int i = 0; i < outKeyList.size(); i++) {

                    context.write(new Text(outKeyList.get(i)), new Text(
                            outValue.toString()));
                }
            }
            else {
                outValue.append("1");
                outValue.append(SEPARATOR);
                outValue.append(fields[FormatPlayFailEnum.MAC_FORMAT.ordinal()]);
                List<String> outKeyList = KeyCombinedDimensionUtil
                        .getOutputKey(fields, groupByColumns);
                for (int i = 0; i < outKeyList.size(); i++) {
                    context.write(new Text(outKeyList.get(i)), new Text(
                            outValue.toString()));
                }

            }

        }

        private boolean fromAppFbuffer(Path filePath) {
            return filePath.toString().contains("fbuffer");
        }
    }

    public static class PlayFailedCalculateReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            long playNum = 0;
            long playFailedNum = 0;
            Set<String> playUserSet = new HashSet<String>();
            Set<String> playFailedUserSet = new HashSet<String>();

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                if (fields.length < 2)
                    continue;
                String tag = fields[0];
                String mac = fields[1];
                if ("0".equals(tag)) {
                    playNum++;
                    playUserSet.add(mac);
                }
                else {
                    playFailedNum++;
                    playFailedUserSet.add(mac);

                }

            }
            StringBuilder outValue = new StringBuilder();
            outValue.append(playNum);
            outValue.append(SEPARATOR);
            outValue.append(playUserSet.size());
            outValue.append(SEPARATOR);
            outValue.append(playFailedNum);
            outValue.append(SEPARATOR);
            outValue.append(playFailedUserSet.size());
            context.write(key, new Text(outValue.toString()));

        }

    }

    /**
     * @throws Exception
     * 
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

        nRet = ToolRunner.run(new Configuration(), new PlayFailedCalculate(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "mobilequality-playfailed-calculate");
        job.setJarByClass(PlayFailedCalculate.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PlayFailedCalculateMapper.class);
        job.setReducerClass(PlayFailedCalculateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}