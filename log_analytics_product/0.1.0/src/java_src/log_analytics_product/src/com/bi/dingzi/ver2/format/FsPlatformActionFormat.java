/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlatformActionFormat.java 
 * @Package com.bi.dingzi.ver2.format 
 * @Description: 钉子、窗帘工具格式化
 * @author fuys
 * @date 2013-7-25 下午4:56:46 
 * @input:输入日志路径/2013-7-25 /dw/logs/tools/origin/FsPlatformAction2/$DIR_DATE
 * @output:输出日志路径/2013-7-25  /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.format.FsPlatformActionFormat --input $DIR_ORGINDATA_DAY_DTAIL_HOUR_INPUT --output /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY --inpulzo 1
 * @inputFormat: PROTOCOL   RPROTOCOL    TIME  IP  ACTION  ACTIONRESULT    ACTIONOBJECTVER   CHANNELID    MAC   GUID NAME   VERSION  ACTIONTIME
 * @ouputFormat: DATEID  HOURID  TIME  IP  ACTION  ACTIONRESULT    ACTIONOBJECTVER   CHANNELID    MAC   GUID NAME   VERSION  ACTIONTIME
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
 * @ClassName: FsPlatformActionFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-25 下午4:56:46
 */
public class FsPlatformActionFormat extends Configured implements Tool {

    enum FsPlatformActionEnum {
        /**
         * 
         * 
         * 
         * "rprotocol", # 日志请求协议版本号，由前端发送，表明前端发送的版本号 "action", #
         * 钉子动作，以字符串形式显示：PullupClient
         * -钉子拉客户端（包括正式版和绿色版），PullupPushTool-钉子拉同步工具，DownloadGclient
         * -钉子下载绿色版客户端，ScreenSaverPullClient
         * -屏保触发钉子拉客户端，ScreenSaverPullDloader-屏保触发钉子拉下载器
         * ，ClockScreenPullupClient-
         * 锁屏触发钉子拉客户端，ClockScreenPullDloader-锁屏触发钉子拉下载器，
         * ScreenSaver-用户机器进入屏保钉子上报，LockScreen-用户机器进入锁屏钉子上报 "actionresult", #
         * 钉子动作行为结果，1X表示拉客户端结果（10：拉起失败，11：拉起成功，12：本地客户端已经启动不拉，13：
         * LastBootedTime与本地时间同一天不拉
         * ），2X表示拉起同步工具（20：android拉起失败，21：android拉起成功，22：
         * ios拉起失败，23：ios拉起成功），3X表示下载绿色版客户端
         * （30：下载失败，31：下载成功），4X表示屏保触发钉子拉客户端（40：拉起失败
         * ，41：拉起成功，42：策略不拉，43：已有不拉，44：屏保退出不拉
         * ），5X表示屏保触发钉子拉下载器（50：下载失败，51：下载成功，52：
         * 本地调用，53：策略不下载），6X表示锁屏触发钉子拉客户端（60：拉起失败
         * ，61：拉起成功，62：策略不拉，63：已有不拉，64：锁屏退出不拉
         * ），7X表示锁屏触发钉子拉下载器（70：下载失败，71：下载成功，72：本地调用，73：策略不下载），用户机器进入屏保和锁屏时该字段为空
         * "actionobjectver", #
         * 钉子交互对象版本，字符串，10-拉起客户端失败时报，client+version（客户端版本）-拉起客户端成功时
         * ，20-拉起同步工具失败时报
         * ，pushtool+version（同步工具版本）-拉起同步工具成功时，screendll+veraion（屏保或锁屏相应的dll版本
         * ）-屏保/锁屏时触发钉子拉客户端或下载器，其余情况此自段为空 "channelid", #
         * 渠道id，当拉起客户端时：00-拉起正式版客户端失败及无客户端
         * ，normal+channelid-拉起正式版客户端成功，10-拉起绿色版客户端失败及无客户端
         * ，green+channelid-拉起绿色版客户端成功；其余情况该字段为空 "mac", # 本机mac地址 "guid", #
         * 计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符） "name", #
         * 钉子名称：FSPAP（优），FSluncher（优）、Fsplatform（劣）、FsSvr（劣） "version", # 钉子版本
         * "actiontime", #
         * 零点操作时机：只有在拉起客户端和下载绿色版客户端时且只在00:10分和00：30有动作时上报动作的时间点：00
         * :10或00:30，其他情况报null
         * 
         * 
         * 
         */

        PROTOCOL, RPROTOCOL, TIME, IP, ACTION, ACTIONRESULT, ACTIONOBJECTVER, CHANNELID, MAC, GUID, NAME, VERSION, ACTIONTIME;
    }

    public static class FsPlatformActionMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    DateFormatInfo.SEPARATOR, 0);
             if (fields.length > FsPlatformActionEnum.VERSION.ordinal()) {
            String timestampInfoStr = fields[FsPlatformActionEnum.TIME
                    .ordinal()];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(timestampInfoStr);
            String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
            String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
            String ipStr = IPFormatUtil.ipFormat(fields[FsPlatformActionEnum.IP
                    .ordinal()]);
            String actionStr = fields[FsPlatformActionEnum.ACTION.ordinal()]
                    .toLowerCase();
            String actionresultStr = fields[FsPlatformActionEnum.ACTIONRESULT
                    .ordinal()];
            String actionobjectverStr = fields[FsPlatformActionEnum.ACTIONOBJECTVER
                    .ordinal()];
            String channelidStr = fields[FsPlatformActionEnum.CHANNELID
                    .ordinal()];
            String macStr = fields[FsPlatformActionEnum.MAC.ordinal()];
            String guidStr = fields[FsPlatformActionEnum.GUID.ordinal()];
            String nameStr = fields[FsPlatformActionEnum.NAME.ordinal()];
            String versionStr = IPFormatUtil
                    .ipFormat(fields[FsPlatformActionEnum.VERSION.ordinal()]);
            String actiontimeStr = fields[FsPlatformActionEnum.ACTIONTIME
                    .ordinal()];
            // versionId
            long versionId = 0l;
            versionId = IPFormatUtil.ip2long(versionStr);

            long actionobjectverId = 0l;
            actionobjectverId = IPFormatUtil.ip2long(actionobjectverStr);

            String outValueStr = dateId + DateFormatInfo.SEPARATOR + hourIdStr
                    + DateFormatInfo.SEPARATOR + ipStr
                    + DateFormatInfo.SEPARATOR + actionStr
                    + DateFormatInfo.SEPARATOR + actionresultStr
                    + DateFormatInfo.SEPARATOR + actionobjectverStr
                    + DateFormatInfo.SEPARATOR + channelidStr
                    + DateFormatInfo.SEPARATOR + macStr
                    + DateFormatInfo.SEPARATOR + guidStr
                    + DateFormatInfo.SEPARATOR + nameStr
                    + DateFormatInfo.SEPARATOR + versionStr
                    + DateFormatInfo.SEPARATOR + actiontimeStr
                    + DateFormatInfo.SEPARATOR + versionId
                    + DateFormatInfo.SEPARATOR + actionobjectverId;
            context.write(new Text(timestampInfoStr), new Text(outValueStr));
        }

         }
    }

    public static class FsPlatformActionReducer extends
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
        commIsLZOArgs.init("fsplatformactionformat.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new FsPlatformActionFormat(), commIsLZOArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_fsplatformactionformat");
        job.setJarByClass(FsPlatformActionFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FsPlatformActionMapper.class);
        job.setReducerClass(FsPlatformActionReducer.class);
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
