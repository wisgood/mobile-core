/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlatFormBootFormat.java 
 * @Package com.bi.dingzi.format 
 * @Description: 钉子启动日志格式化
 * @author fuys
 * @date 2013-7-24 下午2:39:02 
 * @input:输入日志路径/2013-7-24 一天24小时dw/logs/tools/origin/FsPlatformBoot/3/
 * @output:输出日志路径/2013-7-24 /tmp/fys/dingzi/3/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.format.FsPlatFormBootFormat --input $DIR_ORGINDATA_DAY_DTAIL_HOUR_INPUT --output /tmp/fys/dingzi/3/$DIR_DAY --inpulzo 1
 * @inputFormat:PROTOCOL RPROTOCOL   TIME    IP  BOOTMETHOD  CLIENTSTATE     SYNCTOOLSTATE   MAC     GUID    NAME    VERSION OS;
 * @ouputFormat:DATEID  HOURID  IP  BOOTMETHOD  CLIENTSTATE     SYNCTOOLSTATE   MAC     GUID    NAME    VERSION  OS
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
 * @ClassName: FsPlatFormBootFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-24 下午2:39:02
 */
public class FsPlatFormBootFormat extends Configured implements Tool {

    enum FsPlatformBootEnum {
        /**
         * 日志请求协议版本号，由前端发送，表明前端发送的版本号
         * 启动类型：0-未知方式启动钉子，1-服务拉起钉子，2-计划任务拉起钉子，3-ie插件（BHO）
         * 拉起钉子，4-非ie插件（Firefox）拉起钉子（为以后功能预留），5-大安装包启动钉子，6-小安装包启动钉子，7-
         * 双击启动钉子，8-ie插件（Active X）拉起钉子，9-NPAP插件拉起钉子
         * 客户端情况：0-没有客户端，1-有客户端没启动，2-有客户端启动 同步工具状况：0-不存在，1-存在 本机mac地址
         * 计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符）
         * 钉子名称：FSPAP（优），FSluncher（优）、Fsplatform（劣）、FsSvr（劣） 钉子版本
         * 系统类型：other，xp，vista，win7-32，win7-64
         */

        PROTOCOL, RPROTOCOL, TIME, IP, BOOTMETHOD, CLIENTSTATE, SYNCTOOLSTATE, MAC, GUID, NAME, VERSION, OS;
    }

    public static class FsPlatFormBootFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            try {
                String valueStr = value.toString();
                String[] fields = DateFormat.split(valueStr,
                        DateFormatInfo.SEPARATOR, 0);
                String timestampInfoStr = fields[FsPlatformBootEnum.TIME
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                String ipStr = IPFormatUtil
                        .ipFormat(fields[FsPlatformBootEnum.IP.ordinal()]);
                String bootMethodStr = fields[FsPlatformBootEnum.BOOTMETHOD
                        .ordinal()].toLowerCase();
                Long.parseLong(bootMethodStr);
                String clientStateStr = fields[FsPlatformBootEnum.CLIENTSTATE
                        .ordinal()];
                Long.parseLong(clientStateStr);
                String syncToolsstateStr = fields[FsPlatformBootEnum.SYNCTOOLSTATE
                        .ordinal()];
                Long.parseLong(syncToolsstateStr);
                String macStr = fields[FsPlatformBootEnum.MAC.ordinal()];
                String guidStr = fields[FsPlatformBootEnum.GUID.ordinal()];
                String nameStr = fields[FsPlatformBootEnum.NAME.ordinal()]
                        .toLowerCase();
                String versionStr = IPFormatUtil
                        .ipFormat(fields[FsPlatformBootEnum.VERSION.ordinal()]);
                String osStr = fields[FsPlatformBootEnum.OS.ordinal()];
                // versionId
                long versionId = -0l;
                versionId = IPFormatUtil.ip2long(versionStr);
                String outValueStr = dateId + DateFormatInfo.SEPARATOR
                        + hourIdStr + DateFormatInfo.SEPARATOR + ipStr
                        + DateFormatInfo.SEPARATOR + bootMethodStr
                        + DateFormatInfo.SEPARATOR + clientStateStr
                        + DateFormatInfo.SEPARATOR + syncToolsstateStr
                        + DateFormatInfo.SEPARATOR + macStr
                        + DateFormatInfo.SEPARATOR + guidStr
                        + DateFormatInfo.SEPARATOR + nameStr
                        + DateFormatInfo.SEPARATOR + versionStr
                        + DateFormatInfo.SEPARATOR + osStr
                        + DateFormatInfo.SEPARATOR + versionId;
                context.write(new Text(timestampInfoStr), new Text(outValueStr));
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

        }

    }

    public static class FsPlatFormBootFormatReducer extends
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
        commIsLZOArgs.init("fsplatformbootformat.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new FsPlatFormBootFormat(), commIsLZOArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_fsplatformbootformat");
        job.setJarByClass(FsPlatFormBootFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsPlatFormBootFormatMapper.class);
        job.setReducerClass(FsPlatFormBootFormatReducer.class);
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
