/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: GroupByURLSeoImportPageMR.java 
 * @Package com.bi.web.seo 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-7 下午6:04:33 
 * @input:输入日志路径/2013-11-7
 * @output:输出日志路径/2013-11-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.web.seo;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import com.bi.common.util.HdfsUtil;

/**
 * @ClassName: GroupByURLSeoImportPageMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-7 下午6:04:33
 */
public class GroupByURLSeoImportPageMR extends Configured implements Tool {

    public static class GroupByURLSeoImportPageMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = null;

        private Text valueText = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            keyText = new Text();
            valueText = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            keyText.clear();
            valueText.clear();
            String valueStr = value.toString();
            String[] fields = valueStr.split("\t",-1);
            keyText.set(fields[1].trim() + "\t" + fields[0].trim());
            valueText.set(fields[2].trim() + "\t" + fields[3].trim() + "\t"
                    + fields[4].trim() + "\t" + fields[5].trim());
            context.write(keyText, valueText);
        }

    }

    public static class GroupByURLSeoImportPageReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        private Text keyText = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            keyText = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            keyText.clear();
            long pvcount = 0;
            long uvcount = 0;
            double bounceCount = 0;
            double sessionCount = 0;
            for (Text value : values) {
                String valueStr = value.toString();
                String[] fields = valueStr.split("\t", -1);
                try {
                    pvcount += Long.parseLong(fields[0].trim());
                    uvcount += Long.parseLong(fields[1].trim());
                    bounceCount += Double.parseDouble(fields[2]);
                    sessionCount += Double.parseDouble(fields[3]);
                }
                catch(NumberFormatException e) {
                    // TODO Auto-generated catch block
                    System.out.println(key.toString() + "\t" + pvcount + "\t"
                            + uvcount + "\t" + bounceCount + "\t"
                            + sessionCount);
                    e.printStackTrace();
                }

            }
            if (sessionCount > 0) {
                BigDecimal bouncerateBigDecimal = new BigDecimal(bounceCount
                        / sessionCount);
                double bouncerate = bouncerateBigDecimal.setScale(2,
                        BigDecimal.ROUND_HALF_UP).doubleValue();
                keyText.set(key.toString() + "\t" + pvcount + "\t" + uvcount
                        + "\t" + bouncerate);
                context.write(keyText, NullWritable.get());
            }
            else {
                System.out.println(key.toString() + "\t" + pvcount + "\t"
                        + uvcount + "\t" + bounceCount + "\t" + sessionCount);
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
        int res = ToolRunner.run(new Configuration(),
                new GroupByURLSeoImportPageMR(), args);
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
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(GroupByURLSeoImportPageMR.class);
        job.setMapperClass(GroupByURLSeoImportPageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(GroupByURLSeoImportPageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String jobName = job.getConfiguration().get(
                MapReduceConfInfoEnum.jobName.getValueStr());
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                MapReduceConfInfoEnum.inputPath.getValueStr());
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                MapReduceConfInfoEnum.outPutPath.getValueStr());
        System.out.println(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                MapReduceConfInfoEnum.reduceNum.getValueStr(), 0);
        System.out.println(MapReduceConfInfoEnum.reduceNum.getValueStr() + ":"
                + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job
                .getConfiguration()
                .getInt(MapReduceConfInfoEnum.isInputFormatLZOCompress
                        .getValueStr(),
                        1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.waitForCompletion(true);
        return 0;
    }

}
