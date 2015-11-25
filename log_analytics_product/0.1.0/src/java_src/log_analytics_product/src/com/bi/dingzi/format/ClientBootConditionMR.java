/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ClientBootConditionMR.java 
 * @Package com.bi.dingzi.format 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-13 下午3:38:41 
 */
package com.bi.dingzi.format;

import java.io.IOException;

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
import org.apache.log4j.Logger;

import com.bi.common.init.CommArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.StringFormatUtils;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.dingzi.logenum.ClientBootEnum;

/**
 * @ClassName: ClientBootConditionMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-13 下午3:38:41
 */
public class ClientBootConditionMR extends Configured implements Tool {

    
    /**
     * 
     * @ClassName: ClientBootConditionMapper
     * @Description: 杩欓噷鐢ㄤ竴鍙ヨ瘽鎻忚堪杩欎釜绫荤殑浣滅敤
     * @author fuys
     * @date 2013-5-30 涓嬪崍2:31:24
     */
    public static class ClientBootConditionMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static final String STARTTYPE = "0";

        private static Logger logger = Logger
                .getLogger(ClientBootConditionMapper.class.getName());

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String[] fields = value.toString().trim().split(",");
            if (fields.length > ClientBootEnum.STARTTYPE.ordinal()) {
                fields[ClientBootEnum.TIME.ordinal()] = TimestampFormatUtil
                        .formatTimestamp(fields[ClientBootEnum.TIME.ordinal()])
                        .get(ConstantEnum.DATE_ID);
                String macStr = fields[ClientBootEnum.MAC.ordinal()].trim();
                String starttypeStr = fields[ClientBootEnum.STARTTYPE.ordinal()]
                        .trim();
                if (ClientBootConditionMapper.STARTTYPE
                        .equalsIgnoreCase(starttypeStr)) {
                    context.write(
                            new Text(macStr),
                            new Text(StringFormatUtils.arrayToString(fields,
                                    "\t")));
                }
            }
        }
    }

    public static class ClientBootConditionReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Text value : values) {
                context.write(value, NullWritable.get());
            }
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "ClientBootConditionMR");
        job.setJarByClass(ClientBootConditionMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ClientBootConditionMapper.class);
        job.setReducerClass(ClientBootConditionReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        CommArgs commArgs = new CommArgs();
        commArgs.init("clientbootcondition.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new ClientBootConditionMR(), commArgs.getCommsParam());
        System.out.println(res);
    }
}
