/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PVCaculateMR.java 
 * @Package com.bi.web.seo 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-6 下午12:58:12 
 * @input:输入日志路径/2013-11-6
 * @output:输出日志路径/2013-11-6
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.web.seo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.util.HdfsUtil;

/**
 * @ClassName: PVCaculateMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-6 下午12:58:12
 */
public class PVCaculateMR extends Configured implements Tool {
    public static String decodeCodedStr(String sourceStr, String targetCharset)
            throws UnsupportedEncodingException {
        String decodedStr;
        String changedStr = sourceStr; // String changedStr =
                                       // changeCharset(sourceStr,
                                       // targetCharset);
        if (changedStr != null) {
            try {
                decodedStr = URLDecoder.decode(
                        URLDecoder.decode(changedStr, targetCharset),
                        targetCharset);
                // decodedStr = URLDecoder.decode(changedStr, targetCharset);
            }
            catch(Exception e) {
                decodedStr = "";
                return decodedStr;
            }
            return decodedStr;
        }
        return null;
    }

    public static class PVCaculateMapper extends
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
            String[] fields = valueStr.split("\t", -1);
            String url = fields[PvEnum.URL.ordinal()];
            String refferUrl = decodeCodedStr(
                    fields[PvEnum.REFERURL.ordinal()], "utf-8");
            boolean isWebPage = url.indexOf("http://www") == 0;
            try {
                if (isWebPage) {
                    boolean isInstationReff = refferUrl
                            .indexOf("http://www.funshion.com") == 0
                            || refferUrl.indexOf("http://fs.funshion.com") == 0;
                    if (isInstationReff) {
                        keyText.set(new String(refferUrl.substring("http://"
                                .length())));
                    }
                    else {
                        if (refferUrl.indexOf("http://") == 0) {
                            refferUrl = new String(
                                    refferUrl.substring("http://".length()));
                        }
                        boolean isNotSingleWeb = refferUrl.indexOf("/") > 0;
                        if (isNotSingleWeb) {
                            refferUrl = refferUrl.substring(0,
                                    refferUrl.indexOf("/"));
                            keyText.set(refferUrl.contains(":") ? new String(
                                    refferUrl.substring(0,
                                            refferUrl.indexOf(":")))
                                    : refferUrl);
                        }
                    }
                    valueText.set(refferUrl);
                    context.write(keyText, valueText);
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                System.out.print(refferUrl);
                e.printStackTrace();
                return;
            }
        }

    }

    public static class PVCaculateReducer extends
            Reducer<Text, Text, Text, IntWritable> {
        private IntWritable valueInt = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            valueInt = new IntWritable(0);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int sum = 0;
            valueInt.set(0);
            for (Text text : values) {
                sum++;
            }
            valueInt.set(sum);
            context.write(key, valueInt);
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
        int res = ToolRunner.run(new Configuration(), new PVCaculateMR(), args);
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
        job.setJarByClass(PVCaculateMR.class);
        job.setMapperClass(PVCaculateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(PVCaculateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
