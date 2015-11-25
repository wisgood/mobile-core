/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekCountByWeekDay.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-4 下午5:08:57 
 */
package com.bi.dingzi.day.user;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.dingzi.day.user.WeekCountByWeekDayMR.WeekCountByWeekDayMapper;
import com.bi.dingzi.day.user.WeekCountByWeekDayMR.WeekCountByWeekDayReducer;

/**
 * @ClassName: WeekCountByWeekDay
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-4 下午5:08:57
 */
public class WeekCountByWeekDay extends Configured implements Tool {

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
        for(int i=0;i<args.length;i++){
            
            System.out.println(args[i]);
        }
        WeekCountByWeekDayArgs weekCountByWeekDayArgs = new WeekCountByWeekDayArgs();
        int nRet = 0;
        try {
            weekCountByWeekDayArgs.init("weekcountbyweekdaycount.jar");

            weekCountByWeekDayArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            // countArgs.parser.printUsage();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new WeekCountByWeekDay(),
                weekCountByWeekDayArgs.getCountParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_weekcountbyweekdaycount");
        job.setJarByClass(WeekCountByWeekDay.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("date", args[2]);
        job.setMapperClass(WeekCountByWeekDayMapper.class);
        job.setReducerClass(WeekCountByWeekDayReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
