/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PVMuvsConditionMR.java 
 * @Package com.bi.website.mediaplay.tmp.muvs 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-22 下午2:03:15 
 */
package com.bi.website.mediaplay.tmp.muvs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.comm.util.StringDecodeFormatUtil;

/**
 * @ClassName: PVMuvsConditionMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-22 下午2:03:15
 */
public class PVMuvsConditionMR {

    public static class PVMuvsConditionMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String conditionUrl = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.conditionUrl = context.getConfiguration().get("curl");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orginData = value.toString();
            String[] fields = orginData.split(",");
            if (fields.length > PVDataEnum.FCK.ordinal()) {
                String userFCK = fields[PVDataEnum.FCK.ordinal()];
                String timestampStr = fields[PVDataEnum.TIMESTAMP.ordinal()];
                String refferURL = StringDecodeFormatUtil.decodeCodedStr(
                        fields[PVDataEnum.REFERRER.ordinal()], "UTF-8");
                if (!("".equalsIgnoreCase(refferURL))&&(refferURL.contains(this.conditionUrl))) {
                    StringBuilder valueSB = new StringBuilder();
                    valueSB.append(userFCK);
                    valueSB.append("\t");
                    valueSB.append(timestampStr);
                    valueSB.append("\t");
                    valueSB.append(refferURL);
                    context.write(new Text(userFCK),
                            new Text(valueSB.toString()));
                }

            }
            

        }

    }

    public static class PVMuvsConditionReducer extends
            Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Long, Text> timeTree = new TreeMap<Long, Text>();
            for (Text value : values) {
                String valueStr = value.toString();
                String[] valueStrs = valueStr.split("\t");
                timeTree.put(new Long(valueStrs[1]), value);

            }
            context.write(NullWritable.get(), timeTree.firstEntry().getValue());
        }

    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        String conditionUrl = "vas.funshion.com/attachment/editor/minisite/";
        String countInputPaths = "input_pv";
        Job job = new Job();
        job.setJarByClass(PVMuvsConditionMR.class);
        job.setJobName("PVMuvsConditionMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("curl", conditionUrl);
        FileInputFormat.setInputPaths(job, countInputPaths);
        FileOutputFormat.setOutputPath(job, new Path("output_pv_muvs"));
        job.setMapperClass(PVMuvsConditionMapper.class);
        job.setReducerClass(PVMuvsConditionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
