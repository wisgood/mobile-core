/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsActionDateVerFormatMR.java 
 * @Package com.bi.dingzi.format 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-14 下午2:36:29 
 */
package com.bi.dingzi.format;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.init.CommArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.dingzi.logenum.FsPlatformActionEnum;

/**
 * @ClassName: FsActionDateVerFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-14 下午2:36:29
 */
public class FsActionFormatMR extends Configured implements Tool {

    public static class FsActionDateVerFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length > FsPlatformActionEnum.VERSION.ordinal()) {
                try {
                    StringBuilder valueSB = new StringBuilder();
                    String timeStampStr = fields[FsPlatformActionEnum.TIME
                            .ordinal()];
                    String versionInfo = fields[FsPlatformActionEnum.VERSION
                            .ordinal()];
                    // 日期id
                    String dateIdStr = TimestampFormatUtil.formatTimestamp(
                            timeStampStr).get(ConstantEnum.DATE_ID);
                    // versionId
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    valueSB.append(versionId);
                    valueSB.append("\t");
                    // MAC地址
                    String macStr = MACFormatUtil
                            .macFormatToCorrectStr(fields[FsPlatformActionEnum.MAC
                                    .ordinal()]);
                    valueSB.append(macStr);
                    valueSB.append("\t");
                    // 钉子动作行为结果
                    String actionresultStr = fields[FsPlatformActionEnum.ACTIONRESULT
                            .ordinal()];
                    valueSB.append(actionresultStr);
                    valueSB.append("\t");
                    valueSB.append(fields[FsPlatformActionEnum.ACTION.ordinal()]);

                    context.write(new Text(dateIdStr),
                            new Text(valueSB.toString()));
                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

    }

    public static class FsActionDateVerFormatReduce extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            for (Text value : values) {
                context.write(key, value);
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
        CommArgs commArgs = new CommArgs();
        commArgs.init("fsactiondateverformat.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new FsActionFormatMR(),
                commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FsActionFormatMR");
        job.setJarByClass(FsActionFormatMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsActionDateVerFormatMapper.class);
        job.setReducerClass(FsActionDateVerFormatReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
