/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BooStrapMR.java 
 * @Package com.bi.mobile 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-9-24 上午11:05:43 
 * @input:输入日志路径/2013-9-24
 * @output:输出日志路径/2013-9-24
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.mobile;

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

import com.bi.common.util.CommonConstant;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.MACFormatUtil;

/**
 * @ClassName: BooStrapMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-9-24 上午11:05:43
 */
public class BooStrapMR extends Configured implements Tool {

    public static class BootStrapMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        enum BootStrapEnum {

            /*
             * 设备类型(dev)：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号> 
             * 设备mac地址(mac)：长度为16的大写字符串（待确认）  app版本号(ver)：类ip地址的字符串 
             * 网络类型(nt)：1—wifi，2--3g，3—其它 ，-1—无网络  启动方式（btype）:*
             * 0—其它启动；1—手动启动；2—ios平台：推送启动
             * ，android平台：调用播放器播放本地文件；3—ios平台：有角标启动上报（其它启动不包括有角标启动上报
             * ），android平台：推送通知栏启动；4
             * —android平台：后台下载进入下载管理界面；5—android平台：网页调起app；6—android平台
             * ：按home键应用进入后台后再次回到前台
             * ；7—android平台：推送桌面弹窗启动；8—android平台：本地通知启动；9–android平台：通过引入第三方push
             * sdk创建的通知栏启动 10 –android平台：通过引入第三方push sdk创建的桌面弹窗启动 
             * 启动耗时（btime）：从点击到主框架加载完毕耗时，单位：ms  启动是否成功（ok）：1—成功，-1—其它—错误代码 
             * 屏幕分辨率（sr）：屏幕分辨率，N*M  设备内存空间（mem）：单位MB  设备存储空间（tdisk）：单位MB 
             * 设备剩余空间(fdisk)：单位MB  渠道ID(sid):区分各个渠道商  启动时间戳（rt）：unix时间戳 (ipad,
             * iphone) 是否越狱（broken）： (iphone)  设备IMEI（imei）: 设备IMEI号
             * (aphone，apad) 安装时间戳 唯一标识用户 消息id
             */
            TIMESTAMP, IP, DEV, MAC, VER, NT, BTYPE, BTIME, OK, SR, MEM, TDISK, FDISK, SID, RT, IPHONEIP, BROKEN, IMEI, INSTALLT, FUDID, MESSAGEID;

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr, DateFormatInfo.COMMA,
                    0);
            if (fields.length > BootStrapEnum.FUDID.ordinal()) {

                try {
                    fields = recomposeBySpecialVersion(fields,
                            BootStrapEnum.class.getName());
                    String macOrigin = fields[BootStrapEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macOrigin);
                    String macFormat = MACFormatUtil
                            .macFormatToCorrectStr(macOrigin);
                    String fudidStr = fields[BootStrapEnum.FUDID.ordinal()];
                    context.write(new Text(macFormat + DateFormatInfo.COMMA
                            + fudidStr), NullWritable.get());
                }
                catch(ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return;
                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    return;
                }

            }

        }

        public static String[] recomposeBySpecialVersion(String[] splitSts,
                String enumClassStr) throws ClassNotFoundException {

            String[] splitStsValue = splitSts;
            Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
            String versionInfo = splitStsValue[Enum.valueOf(logEnum, "SID")
                    .ordinal() - 1];
            if ("1.2.0.2".equalsIgnoreCase(versionInfo)
                    || "1.2.0.1".equalsIgnoreCase(versionInfo)) {

                splitStsValue = new String[splitSts.length];

                for (int i = 0; i < Enum.valueOf(logEnum, "VER").ordinal(); i++) {
                    splitStsValue[i] = splitSts[i];
                }
                splitStsValue[Enum.valueOf(logEnum, "VER").ordinal()] = versionInfo;
                for (int i = Enum.valueOf(logEnum, "VER").ordinal(); i < splitSts.length; i++) {
                    if (i < splitSts.length - 1) {
                        splitStsValue[i + 1] = splitSts[i];
                    }
                    else {
                        splitStsValue[i] = splitSts[i];
                    }
                }
            }
            return splitStsValue;
        }
    }

    public static class BootStrapReducer extends
            Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            //System.out.println(key.toString());
            context.write(key, NullWritable.get());
        }

    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int res = ToolRunner.run(new Configuration(), new BooStrapMR(), args);
        System.out.println(res);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        System.out.println("hello world1");
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(BooStrapMR.class);
        job.setMapperClass(BootStrapMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(BootStrapReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobiletmp_BooStrapMR_" + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
        System.out.println(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
        System.out.println(CommonConstant.REDUCE_NUM + ":" + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job.getConfiguration().getInt(
                CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.waitForCompletion(true);
        return 0;
    }

}
