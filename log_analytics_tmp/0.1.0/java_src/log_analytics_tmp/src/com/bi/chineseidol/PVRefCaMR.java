/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PVRefCaMR.java 
 * @Package com.bi.chineseidol 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-28 上午10:26:12 
 */
package com.bi.chineseidol;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: PVRefCaMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-28 上午10:26:12
 */
public class PVRefCaMR extends Configured implements Tool {

    public static class PVRefCaMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private String curlStr = null;

        private final static int ONE = 1;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.curlStr = context.getConfiguration().get("curl");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length > PvEnum.REFERURL.ordinal()) {
                String urlStr = fields[PvEnum.URL.ordinal()];
                String[] curls = this.curlStr.split(",");
                if (!"".equalsIgnoreCase(urlStr)
                        && (urlStr.contains(curls[0]) || urlStr
                                .contains(curls[1]))
                        || urlStr.contains(curls[2])) {
                    String referURL = fields[PvEnum.REFERURL.ordinal()];
                    if (!"".equalsIgnoreCase(referURL)
                            && referURL.contains("funshion")) {
                        context.write(new Text(referURL), new IntWritable(1));
                    }

                }
            }
        }
    }

    public static class PVRefCaReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "PVRefCaMR");
        job.setJarByClass(PVRefCaMapper.class);
        job.getConfiguration().set("curl", args[2]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PVRefCaMapper.class);
        job.setCombinerClass(PVRefCaReducer.class);
        job.setReducerClass(PVRefCaReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        // FileOutputFormat.setCompressOutput(job, true);// 设置输出结果采用压缩
        // FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    /**
     * 
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args
     * @param @throws Exception 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        PVMuvsConditionArgs pvmuvsConditionArgs = new PVMuvsConditionArgs();
        int nRet = 0;

        try {
            pvmuvsConditionArgs.init("pvrefcamr.jar");
            pvmuvsConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            // e.printStackTrace();
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new PVRefCaMR(),
                pvmuvsConditionArgs.getOkParam());
        System.out.println(nRet);
    }
}
