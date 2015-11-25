/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ProportionByColMRUTL.java 
 * @Package com.bi.comm.calculate.proportion 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-13 下午2:30:54 
 */
package com.bi.comm.calculate.proportion;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.mobile.comm.constant.ConstantEnum;

/**
 * @ClassName: ProportionByColMRUTL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-13 下午2:30:54
 */
public class ProportionByColMRUTL {

    public static class ProportionByColMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(ProportionByColMapper.class.getName());

        private String numerator = null;

        private String fileName = null;

        private String[] colNum = null;

        private String caculColNum = null;

        @Override
        public void setup(Context context) {
            FileSplit fs = (FileSplit) context.getInputSplit();
            this.fileName = fs.getPath().toUri().getPath();
            this.numerator = context.getConfiguration().get("numerator");
            this.colNum = context.getConfiguration().get("groupby").split(",");
            this.caculColNum = context.getConfiguration().get("cacul");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder colKeySb = new StringBuilder();
            String[] field = value.toString().trim().split("\t");
            for (int i = 0; i < colNum.length; i++) {
                colKeySb.append(field[Integer.parseInt(colNum[i])]);
                if (i < colNum.length - 1) {

                    colKeySb.append("\t");
                }
            }
            String caculStr = field[Integer.parseInt(caculColNum)];
            if (fileName.contains(this.numerator)) {
                logger.info(ConstantEnum.NUMERATOR + caculStr);
                context.write(new Text(colKeySb.toString()), new Text(
                        ConstantEnum.NUMERATOR + caculStr));

            }
            else {
                logger.info(ConstantEnum.DENOMINATOR + caculStr);
                context.write(new Text(colKeySb.toString()), new Text(
                        ConstantEnum.DENOMINATOR + caculStr));

            }
        }
    }

    /**
     * reduce : sum
     */

    public static class ProportionByColReducer extends
            Reducer<Text, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(ProportionByColReducer.class.getName());

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double numerator = 0;
            double denominator = 0;
            double proportion = 0;
            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.contains(ConstantEnum.NUMERATOR.toString())) {
                     numerator = Double.parseDouble(new String(valueStr
                            .substring(ConstantEnum.NUMERATOR.toString()
                                    .length())));
                     logger.info("fenzi:"+numerator);
                }
                if (valueStr.contains(ConstantEnum.DENOMINATOR.toString())) {
                     denominator = Double.parseDouble(new String(valueStr
                            .substring(ConstantEnum.DENOMINATOR.toString()
                                    .length())));
                     logger.info("fenmu:"+numerator);
                   
                }
            }
            if (0 != denominator) {

                proportion = (numerator / denominator);
            }
            context.write(key, new Text(proportion + ""));

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
        Job job = new Job();
        job.setJarByClass(ProportionByColMRUTL.class);
        job.setJobName("ProportionByColMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("groupby", "0,1,2,3");
        job.getConfiguration().set("numerator", "fenzi");
        job.getConfiguration().set("cacul", "4");
        FileInputFormat
                .setInputPaths(
                        job,
                        "fenzi,fenmu");
        FileOutputFormat.setOutputPath(job, new Path(
                "output_pro"));
        job.setMapperClass(ProportionByColMapper.class);
         job.setReducerClass(ProportionByColReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
