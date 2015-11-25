/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekCountByWeekDayMR.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-4 下午4:56:15 
 */
package com.bi.dingzi.day.user;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @ClassName: WeekCountByWeekDayMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-4 下午4:56:15
 */
public class WeekCountByWeekDayMR {

    public static class WeekCountByWeekDayMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private String endDateStr = null;

        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.endDateStr = context.getConfiguration().get("date");

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            context.write(new Text(this.endDateStr), one);
        }

    }

    public static class WeekCountByWeekDayReducer extends
            Reducer<Text, IntWritable, Text, LongWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            long sum = 0l;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }

    }
}
