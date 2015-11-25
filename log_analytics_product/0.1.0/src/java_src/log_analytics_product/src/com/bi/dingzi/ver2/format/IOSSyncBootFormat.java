/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: SyncBoot.java 
 * @Package com.bi.dingzi.ver2.format 
 * @Description: IOS同步日志格式化
 * @author fuys
 * @date 2013-7-30 上午10:18:38 
 * @input:输入日志路径/2013-7-30 /dw/logs/tools/origin/sync_boot
 * @output:输出日志路径/2013-7-30 /dw/logs/tools/result/ver2/IOSSyncBoot/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.format.IOSSyncBootFormat --input $DIR_ORGINDATA_DAY_DTAIL_HOUR_INPUT --output /dw/logs/tools/result/ver2/IOSSyncBoot/$DIR_DAY --inpulzo 1
 * @inputFormat:PROTOCOL  RPROTOCOL  TIME  IP  APPTYPE  RUN  MAC  GUID 
 * @ouputFormat:DateId HOUR IP APPTYPE RUN MAC GUID
 */
package com.bi.dingzi.ver2.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import com.bi.common.init.CommIsLZOArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: SyncBoot
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-30 上午10:18:38
 */
public class IOSSyncBootFormat extends Configured implements Tool {

    enum SyncBootEnum {

        /**
         * 协议版本号 请求协议版本号 日志记录时间 用户ip 工具类型 启动是否成功 本机mac地址 计算机唯一标
         * 
         */

        PROTOCOL, RPROTOCOL, TIME, IP, APPTYPE, RUN, MAC, GUID;
    }

    public static class IOSSyncBootMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields =  DateFormat.split(valueStr, DateFormatInfo.SEPARATOR, 0);
//            if (fields.length > SyncBootEnum.GUID.ordinal()) {
                String timestampInfoStr = fields[SyncBootEnum.TIME.ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                String ipStr = IPFormatUtil.ipFormat(fields[SyncBootEnum.IP
                        .ordinal()]);
                String apptypeStr = fields[SyncBootEnum.APPTYPE.ordinal()]
                        .toLowerCase();
                String runStr = fields[SyncBootEnum.RUN.ordinal()];

                String macStr = fields[SyncBootEnum.MAC.ordinal()];
                String guidStr = fields[SyncBootEnum.GUID.ordinal()];

                String outValueStr = dateId + DateFormatInfo.SEPARATOR
                        + hourIdStr + DateFormatInfo.SEPARATOR + ipStr
                        + DateFormatInfo.SEPARATOR + apptypeStr
                        + DateFormatInfo.SEPARATOR + runStr
                        + DateFormatInfo.SEPARATOR + macStr
                        + DateFormatInfo.SEPARATOR + guidStr;
                if (null != DateFormat.getAppType("2", apptypeStr)) {
                    context.write(new Text(timestampInfoStr), new Text(
                            outValueStr));
                }
            }

//        }

    }

    public static class IOSSyncBootReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Text value : values) {
                context.write(value, NullWritable.get());
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
        CommIsLZOArgs commIsLZOArgs = new CommIsLZOArgs();
        commIsLZOArgs.init("suncbootformat.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new IOSSyncBootFormat(),
                commIsLZOArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_iossyncbootformat");
        job.setJarByClass(IOSSyncBootFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IOSSyncBootMapper.class);
        job.setReducerClass(IOSSyncBootReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        if (null != args[2] && "1".equalsIgnoreCase(args[2].trim())) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
