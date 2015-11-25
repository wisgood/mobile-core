/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ClientBootHandBootCompute.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 客户端拉起计算
 * @author fuys
 * @date 2013-8-7 上午10:26:03 
 * @input:输入日志路径/2013-8-7 dw/logs/client/origin/boot/$DIR_DAY
 * @output:输出日志路径/2013-8-7 /dw/logs/tools/result/ver2/day/ClientBootHandBoot_user_count/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.ClientBootHandBoot --input /dw/logs/client/origin/boot/$DIR_DAY --output /dw/logs/tools/result/ver2/day/ClientBootHandBoot_user_count/$DIR_DAY       --inpulzo 0
 * @inputFormat:TIME     IP  MAC     VER     CHANNEL     OS      STARTTYPE   ERRORCODE       HC      HARDWARE_ACCELERATION       YY      TRAY_LIMIT
 * @ouputFormat:DateId CLENT_QD_USER_NUM    CLENT_QD_USER_H_NUM    
 */
package com.bi.dingzi.ver2.compute;

import java.io.IOException;
import java.util.HashSet;

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

import com.bi.common.init.CommIsLZOArgs;
import com.bi.common.init.ConstantEnum;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: ClientBootHandBootCompute
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-7 上午10:26:03
 */
public class ClientBootHandBoot extends Configured implements Tool {

    enum ClientBootHandBootEnum {
        /**
         * 日志记录时间 用户ip mac地址 客户端版本号 渠道id 用户pc操作系统版本 启动方式 UI与底层交互错误号 发送hello消息次数
         * 是否启动高清加速 风行语言 是否托盘限速
         */
        TIME, IP, MAC, VER, CHANNEL, OS, STARTTYPE, ERRORCODE, HC, HARDWARE_ACCELERATION, YY, TRAY_LIMIT;

    }

    public static class ClientBootHandBootMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr, ',', 0);
            if (fields.length > ClientBootHandBootEnum.STARTTYPE.ordinal()) {
                String timestampInfoStr = fields[ClientBootHandBootEnum.TIME
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);

                String macInfor = MACFormatUtil
                        .macFormatToCorrectStr(fields[ClientBootHandBootEnum.MAC
                                .ordinal()]);
                String startypeStr = fields[ClientBootHandBootEnum.STARTTYPE
                        .ordinal()];
                String outValueStr = macInfor + DateFormatInfo.SEPARATOR
                        + startypeStr;

                context.write(new Text(dateId), new Text(outValueStr));

            }

        }

    }

    public static class ClientBootHandReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            HashSet<String> totalMacHashSet = new HashSet<String>();
            HashSet<String> handbootMacHashSet = new HashSet<String>();
            for (Text value : values) {
                String valueStr = value.toString();
                String[] fields = DateFormat.split(valueStr,
                        DateFormatInfo.SEPARATOR, 0);
                String macInfor = fields[0];
                totalMacHashSet.add(macInfor);
                String startypeStr = fields[1];
                if (null != startypeStr && startypeStr.equalsIgnoreCase("0")) {
                    handbootMacHashSet.add(macInfor);
                }
            }
            StringBuilder resultSB = new StringBuilder();
            resultSB.append(totalMacHashSet.size());
            resultSB.append(DateFormatInfo.SEPARATOR);
            resultSB.append(handbootMacHashSet.size());
            context.write(key, new Text(resultSB.toString()));
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
        commIsLZOArgs.init("clientboothandcount.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(), new ClientBootHandBoot(),
                commIsLZOArgs.getCommsParam());
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
        Job job = new Job(conf, "5_product_dingzi_clientboothandbootcount");
        job.setJarByClass(ClientBootHandBoot.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ClientBootHandBootMapper.class);
        job.setReducerClass(ClientBootHandReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (null != args[2] && "1".equalsIgnoreCase(args[2].trim())) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
