/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ClientOnlineUserCount.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 客户端在线用户数计算
 * @author fuys
 * @date 2013-8-12 上午10:29:20 
 * @input:输入日志路径/2013-8-12  /hs_log/onlineofflinelog/$DIR_DAY
 * @output:输出日志路径/2013-8-12 /dw/logs/tools/result/ver2/day/clientonlinecount/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.ClientOnlineUserCount --input /hs_log/onlineofflinelog/$DIR_DAY --output /dw/logs/tools/result/ver2/day/clientonlinecount/$DIR_DAY --inpulzo 0
 * @inputFormat:？
 * @ouputFormat:DateId USER_NUM
 */
package com.bi.dingzi.ver2.compute;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.init.CommIsLZOArgs;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;

/**
 * @ClassName: ClientOnlineUserCount
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-12 上午10:29:20
 */
public class ClientOnlineUserCount extends Configured implements Tool {

    public static class ClientOnlineUserCountMapper extends
            Mapper<LongWritable, Text, IntWritable, Text> {
        private String dateIdStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filePathStr = fileSplit.getPath().getParent().toString();
            this.dateIdStr = DateFormat.getDateIdFormPath(filePathStr);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr, DateFormatInfo.COMMA,
                    0);
            context.write(new IntWritable(new Integer(this.dateIdStr)),
                    new Text(fields[11]));

        }

    }

    public static class ClientOnlineUserCountReducer extends
            Reducer<IntWritable, Text, IntWritable, LongWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            HashSet<String> maccodeSet = new HashSet<String>();
            long count = 0l;
            for (Text value : values) {
                String maccodeStr = value.toString();
                if (!maccodeSet.contains(maccodeStr)) {
                    count++;
                    maccodeSet.add(maccodeStr);
                }
            }
            context.write(key, new LongWritable(count));
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
        CommIsLZOArgs commIsLZOArgs = new CommIsLZOArgs();
        commIsLZOArgs.init("clinentonlineusercount.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new ClientOnlineUserCount(), commIsLZOArgs.getCommsParam());
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
        Job job = new Job(conf, "5_product_dingzi_clientonlineusercount");
        job.setJarByClass(ToolsCompute.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ClientOnlineUserCountMapper.class);
        job.setReducerClass(ClientOnlineUserCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(8);
        if (null != args[2] && "1".equalsIgnoreCase(args[2].trim())) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
