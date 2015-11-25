/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: WeekMacMR.java 
 * @Package com.bi.mobilecoredata.middle.week 
 * @Description: 计算周MAC
 * @author fuys
 * @date 2013-7-5 下午4:48:06 
 * @input:输入日志路径/2013-7-5  一周目录 /dw/logs/mobile/result/dayuser/dayuser_commcol_distinct/
 * @output:输出日志路径/2013-7-5 /dw/logs/mobile/result/week/weekmac/
 * @executeCmd:hadoop jar log_analytics.jar com.bi.mobilecoredata.middle.week.WeekMacMR --input $DIR_COREDATA_DAY_DTAIL_INPUT --output /dw/logs/mobile/result/week/weekmac/$DIR_WEEKDATE  --dateid  $WEEKDATE
 * @inputFormat:DATE  HOUR  PLAT  VERSION  QUDAO  CITY  MAC  TIMESTAMP
 * @ouputFormat:DATE_ID QUDAO PLAT MAC LOGINDAYS
 */
package com.bi.mobilecoredata.middle.week;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeekMacMR extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    /**
     * the input log form
     */
    private enum Log {
        DATE, HOUR, PLAT, VERSION, QUDAO, CITY, MAC, TIMESTAMP
    }

    public static class WeekMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            // String outputKey = fields[Log.QUDAO.ordinal()] + SEPARATOR
            // + fields[Log.PLAT.ordinal()] + SEPARATOR
            // + fields[Log.MAC.ordinal()];
            // String outputValue = fields[Log.DATE.ordinal()];//
            // fields[Log.DATE.ordinal()];

            String outputKey = fields[Log.MAC.ordinal()];
            String outputValue = fields[Log.DATE.ordinal()] + SEPARATOR
                    + fields[Log.QUDAO.ordinal()] + SEPARATOR
                    + fields[Log.PLAT.ordinal()];// fields[Log.DATE.ordinal()];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class WeekMacReducer extends Reducer<Text, Text, Text, Text> {
        private String firstDayOfWeek = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.firstDayOfWeek = context.getConfiguration().get("dateid");
        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // DATE_ID QUDAO PLAT MAC
            TreeMap<String, String> loginDaysMap = new TreeMap<String, String>();
            for (Text value : values) {
                String valueStr = value.toString();
                String[] fields = valueStr.split(SEPARATOR);
                String dateIdStr = fields[0];
                String demStr = fields[1] + SEPARATOR + fields[2];
                if (!loginDaysMap.containsKey(dateIdStr)) {
                    loginDaysMap.put(dateIdStr, demStr);
                }
            }
            String outValueStr = key.toString() + SEPARATOR
                    + loginDaysMap.size();
            context.write(new Text(this.firstDayOfWeek + SEPARATOR
                    + loginDaysMap.get(loginDaysMap.firstKey())), new Text(
                    outValueStr));

        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_weekmac");
        job.setJarByClass(WeekMacMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().set("dateid", args[2]);
        job.setMapperClass(WeekMacMapper.class);
        job.setReducerClass(WeekMacReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
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
        WeekUserDetailArgs weekUserDetailArgs = new WeekUserDetailArgs();
        weekUserDetailArgs.init("weekmac.jar");
        weekUserDetailArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new WeekMacMR(),
                weekUserDetailArgs.getCommsParam());
        System.out.println(res);
    }

}
