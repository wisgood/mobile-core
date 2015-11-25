package com.bi.mobilecoredata.middle.pushreach.messtype.condition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.mobile.pushreach.format.dataenum.PushReachFormatEnum;

/**
 * 
 * @ClassName: PushReachMessTypeConditionMR
 * @Description: 取出满足messtype条件的pushReach记录
 * @author fuys
 * @date 2013-5-7 下午4:18:22
 */
public class PushReachMessTypeConditionMR {

    public static class PushReachMessTypeConditionMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static Logger logger = Logger
                .getLogger(PushReachMessTypeConditionMapper.class.getName());

        private String[] conditions = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.conditions = context.getConfiguration().get("conditions")
                    .split(",");

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String orgiDataStr = value.toString();
                String[] splitSts = orgiDataStr.split("\t");
                String messageTypeStr = splitSts[PushReachFormatEnum.MESSAGETYPE
                        .ordinal()];
                if (this.conditions.length > 1
                        && (messageTypeStr.equalsIgnoreCase(this.conditions[0]) || messageTypeStr
                                .equalsIgnoreCase(this.conditions[1]))) {
                    context.write(new Text(
                            splitSts[PushReachFormatEnum.TIMESTAMP.ordinal()]),
                            new Text(orgiDataStr));
                }
                if (this.conditions.length == 1
                        && messageTypeStr.equalsIgnoreCase(this.conditions[0])) {
                    context.write(new Text(
                            splitSts[PushReachFormatEnum.TIMESTAMP.ordinal()]),
                            new Text(orgiDataStr));
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class PushReachMessTypeConditionReduce extends
            Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, NullWritable.get());

            }

        }
    }

    /**
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        // String conditionstr = "1,3";
        // String conditionstr = "2,3";
        // String conditionstr = "1";
        String conditionstr = "3";
        Job job = new Job();
        job.setJarByClass(PushReachMessTypeConditionMR.class);
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        job.getConfiguration().set("conditions", conditionstr);
        FileInputFormat.addInputPath(job, new Path("output_pushreach"));
        FileOutputFormat.setOutputPath(job, new Path(
                "output_pushreach_condition_3"));
        job.setMapperClass(PushReachMessTypeConditionMapper.class);
        job.setReducerClass(PushReachMessTypeConditionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
