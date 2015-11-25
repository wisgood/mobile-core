/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BrowserFormatMR.java 
 * @Package com.bi.dingzi.format 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-14 下午4:48:53 
 */
package com.bi.dingzi.format;




import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import com.bi.common.init.CommArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.dingzi.logenum.BrowserComRunEnum;

/**
 * @ClassName: BrowserFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-14 下午4:48:53
 */
public class BrowserFormatMR extends Configured implements Tool {

    public static class BrowserFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private Pattern pattern = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.pattern = Pattern.compile("\\d+");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length > BrowserComRunEnum.STRATERY.ordinal()) {
                StringBuilder valueSB = new StringBuilder();
                String timeStampStr = fields[BrowserComRunEnum.TIME.ordinal()];
                String versionInfo = fields[BrowserComRunEnum.VERSION.ordinal()];
                String categoryInfo = fields[BrowserComRunEnum.CATEGORY
                        .ordinal()];
                String sucInfo = fields[BrowserComRunEnum.SUC.ordinal()];
                String typeInfo = fields[BrowserComRunEnum.TYPE.ordinal()];
                String macInfo = MACFormatUtil
                        .macFormatToCorrectStr(fields[BrowserComRunEnum.MAC
                                .ordinal()]);
                String stratery = fields[BrowserComRunEnum.STRATERY.ordinal()];
                this.pattern.matcher(categoryInfo);

                if (this.pattern.matcher(categoryInfo).matches()
                        && this.pattern.matcher(sucInfo).matches()
                        && this.pattern.matcher(typeInfo).matches()) {
                    // 日期id
                    String dateIdStr = TimestampFormatUtil.formatTimestamp(
                            timeStampStr).get(ConstantEnum.DATE_ID);
                    valueSB.append(categoryInfo);
                    valueSB.append("\t");
                    // versionId
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    valueSB.append(versionId);
                    valueSB.append("\t");
                    valueSB.append(macInfo);
                    valueSB.append("\t");
                    valueSB.append(sucInfo);
                    valueSB.append("\t");
                    valueSB.append(typeInfo);
                    valueSB.append("\t");
                    valueSB.append(stratery);
                    context.write(new Text(dateIdStr),
                            new Text(valueSB.toString()));
                }

            }
        }
    }

    public static class BrowserFormatReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Text value : values) {

                context.write(key, value);
            }
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
        CommArgs commArgs = new CommArgs();
        commArgs.init("browserformat.jar");
        commArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new BrowserFormatMR(),
                commArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "BrowserFormatMR");
        job.setJarByClass(BrowserFormatMR.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BrowserFormatMapper.class);
        job.setReducerClass(BrowserFormatReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
