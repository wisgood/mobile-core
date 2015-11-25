/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: NoStuckMR.java 
 * @Package com.bi.calculate 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-4 下午5:25:32 
 * @input:输入日志路径/2013-9-4
 * @output:输出日志路径/2013-9-4
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.calculate;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.KeyCombinedDimensionUtil;

/**
 * @ClassName: NoStuckMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-4 下午5:25:32
 */
public class PlayTMNoStuckMR extends Configured implements Tool {

    public static class PlayTMNoStuckMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {
        private int[] groupByColumns = { 0, 1, 2, 3, 4, 5, 7 };

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            try {
                String[] field = DataFormatUtils.split(value.toString(),
                        DataFormatUtils.TAB_SEPARATOR, 0);
                List<String> outKeyList = KeyCombinedDimensionUtil
                        .getOutputKey(field, this.groupByColumns);
                if (context.getConfiguration().getInt("cl", 0) == 1) {
                    int cl = Integer
                            .parseInt(field[com.bi.common.logenum.FormatPlayTMEnum.CL
                                    .ordinal()]);
                    if ((cl > 0) && (cl < 4)) {
                        List<String> outKeyCLList = new ArrayList<String>(
                                outKeyList.size());
                        for (int i = 0; i < outKeyList.size(); i++) {
                            String keyCLStr = ((String) outKeyList.get(i))
                                    .trim()
                                    + DataFormatUtils.TAB_SEPARATOR
                                    + field[com.bi.common.logenum.FormatPlayTMEnum.CL
                                            .ordinal()];
                            outKeyCLList.add(keyCLStr);
                        }
                        outKeyList = outKeyCLList;
                    }
                    else {
                        return;
                    }

                }

                long pnlong = Long
                        .parseLong(field[com.bi.common.logenum.FormatPlayTMEnum.PN_FORMAT
                                .ordinal()]);
                for (int i = 0; i < outKeyList.size(); i++) {
                    context.write(
                            new Text(((String) outKeyList.get(i)).trim()),
                            new LongWritable(pnlong));
                }

            }
            catch(Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public static class PlayTMNoStuckReducer extends
            Reducer<Text, LongWritable, Text, Text> {

        private int sucktNum = 0;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            // Dstucknum

            sucktNum = context.getConfiguration().getInt("stucknum", 0);
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            long nosucklong = 0;
            long totallong = 0;
            for (LongWritable value : values) {
                long sucktvalue = value.get();
                if (sucktvalue <= sucktNum) {
                    nosucklong++;
                }
                totallong++;
            }
            if (0 != totallong) {
                context.write(new Text(key.toString()
                        + DataFormatUtils.TAB_SEPARATOR + sucktNum), new Text(
                        nosucklong + "" + DataFormatUtils.TAB_SEPARATOR
                                + totallong));
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
        int res = ToolRunner.run(new Configuration(), new PlayTMNoStuckMR(),
                args);
        System.out.println(res);
    }

    /**
     * (非 Javadoc)
     * <p>
     * Title: run
     * </p>
     * <p>
     * Description:
     * </p>
     * 
     * @param arg0
     * @return
     * @throws Exception
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        for (int i = 0; i < args.length; i++) {

            System.out.println(i + ":" + args[i]);
        }
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(PlayTMNoStuckMR.class);
        job.setMapperClass(PlayTMNoStuckMapper.class);
        // Text, LongWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(PlayTMNoStuckReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_PlayTMNoStuck_" + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
        System.out.println(CommonConstant.REDUCE_NUM + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job.getConfiguration().getInt(
                CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }

        job.waitForCompletion(true);
        return 0;
    }

}
