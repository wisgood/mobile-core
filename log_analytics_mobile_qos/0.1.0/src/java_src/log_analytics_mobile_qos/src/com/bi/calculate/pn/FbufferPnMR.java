/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FbufferPnMR.java 
 * @Package com.bi.calculate.pn 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-7 下午12:02:14 
 * @input:输入日志路径/2013-9-7
 * @output:输出日志路径/2013-9-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.calculate.pn;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

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
import com.bi.common.constant.DimensionConstant;
import com.bi.common.logenum.FormatFbufferEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.KeyCombinedDimensionUtil;
import com.bi.common.util.PercenTilesUtil;

/**
 * @ClassName: FbufferPnMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-7 下午12:02:14
 */
public class FbufferPnMR extends Configured implements Tool {

    public static class FbufferPnPnMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {

        private int containsHour = 0;

        private int[] dayGroupByColumns = { 0, 2, 3, 4, 5, 7 };

        private int[] hourGroupByColumns = { 0, 1, 2, 3 };

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            containsHour = context.getConfiguration().getInt("containsHour", 0);
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] field = DataFormatUtils.split(value.toString(),
                    DataFormatUtils.TAB_SEPARATOR, 0);

            try {

                List<String> outKeyList = null;
                if (0 == containsHour) {
                    outKeyList = KeyCombinedDimensionUtil.getOutputKey(field,
                            dayGroupByColumns);

                }
                else {
                    outKeyList = KeyCombinedDimensionUtil.getOutputKey(field,
                            hourGroupByColumns);

                }

                int ok = Integer
                        .parseInt(field[FormatFbufferEnum.OK.ordinal()]);
                double btmDouble = 0;

                btmDouble = Double
                        .parseDouble(field[FormatFbufferEnum.BTM_FORMAT
                                .ordinal()]);
                if (1 == context.getConfiguration().getInt("success", 1)) {

                    if ((ok == 0) || (ok == -3 && btmDouble < 45000)
                            || (ok == -7 && btmDouble < 45000)) {

                        for (int i = 0; i < outKeyList.size(); i++) {
                            context.write(new Text(outKeyList.get(i)),
                                    new LongWritable((long) btmDouble));
                        }
                    }

                }

                else {
                    if ((ok == 0) || (ok == -3 && btmDouble < 45000)
                            || (ok == -7 && btmDouble < 45000)) {
                        return;
                    }
                    else {
                        for (int i = 0; i < outKeyList.size(); i++) {
                            context.write(new Text(outKeyList.get(i)),
                                    new LongWritable((long) btmDouble));
                        }
                    }

                }
            }
            catch(Exception e) {

                return;
            }

        }
    }

    public static class FbufferPnPnReducer extends
            Reducer<Text, LongWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            TreeMap<Long, Long> treemap = new TreeMap<Long, Long>();
            int sumcount = 0;
            for (LongWritable value : values) {
                sumcount++;
                long tmpKey = value.get();
                if (treemap.containsKey(tmpKey)) {
                    long tmpCount = treemap.get(tmpKey) + 1;
                    treemap.put(tmpKey, tmpCount);
                }
                else {
                    treemap.put(tmpKey, 1L);
                }
            }
            List<Long> pnList = PercenTilesUtil.calculatePn(treemap, sumcount);
            for (int i = 5; i <= 95; i = i + 5) {

                context.write(key, new Text(i + ""
                        + DataFormatUtils.TAB_SEPARATOR + pnList.get(i)));
            }
            context.write(key, new Text(99 + "" + DataFormatUtils.TAB_SEPARATOR
                    + pnList.get(99)));
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
        int res = ToolRunner.run(new Configuration(), new FbufferPnMR(), args);
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
     * @param args
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
        job.setJarByClass(FbufferPnMR.class);
        System.out.println("groupby:" + job.getConfiguration().get("groupby"));
        job.setMapperClass(FbufferPnPnMapper.class);
        // Text, LongWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(FbufferPnPnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String jobName = job.getConfiguration().get("jobName");
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
        System.out.println(outputPathStr);
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
