/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ClientBootFormat.java 
 * @Package com.bi.dingzi.ver2.format 
 * @Description: 客户端工具格式化
 * @author fuys
 * @date 2013-8-1 上午10:48:28 
 * @input:输入日志路径/2013-8-1  /dw/logs/client/origin/boot/
 * @output:输出日志路径/2013-8-1 /dw/logs/tools/result/ver2/ClientBoot/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.format.ClientBootFormat --input $DIR_ORGINDATA_DAY_DTAIL_HOUR_INPUT --output /dw/logs/tools/result/ver2/ClientBoot/$DIR_DAY --inpulzo 0
 * @inputFormat:TIME, IP, MAC, VER, CHANNEL, OS, STARTTYPE, ERRORCODE, HC, HARDWARE_ACCELERATION, YY, TRAY_LIMIT;
 * @ouputFormat:DateId HOUR IP MAC
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
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;


/**
 * @ClassName: ClientBootFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-1 上午10:48:28
 */
public class ClientBootFormat  extends Configured implements Tool {

    enum ClientBootEnum {
        /**
         * 日志记录时间 用户ip mac地址 客户端版本号 渠道id 用户pc操作系统版本 启动方式 UI与底层交互错误号 发送hello消息次数
         * 是否启动高清加速 风行语言 是否托盘限速
         */
        TIME, IP, MAC, VER, CHANNEL, OS, STARTTYPE, ERRORCODE, HC, HARDWARE_ACCELERATION, YY, TRAY_LIMIT;

    }

    public static class ClientBootMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    ',', 0);
            if (fields.length > ClientBootEnum.MAC.ordinal()) {
                String timestampInfoStr = fields[ClientBootEnum.TIME.ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                String ipStr = IPFormatUtil.ipFormat(fields[ClientBootEnum.IP
                        .ordinal()]);
                String macInfor = MACFormatUtil
                        .macFormatToCorrectStr(fields[ClientBootEnum.MAC
                                .ordinal()]);
                String outValueStr = dateId + DateFormatInfo.SEPARATOR
                        + hourIdStr + DateFormatInfo.SEPARATOR + ipStr
                        + DateFormatInfo.SEPARATOR + macInfor;
                context.write(new Text(timestampInfoStr), new Text(outValueStr));
            }

        }

    }

    public static class ClientBootReducer extends
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
        commIsLZOArgs.init("clientboot.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new ClientBootFormat(),
                commIsLZOArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_clientbootformat");
        job.setJarByClass(ClientBootFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ClientBootMapper.class);
        job.setReducerClass(ClientBootReducer.class);
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
