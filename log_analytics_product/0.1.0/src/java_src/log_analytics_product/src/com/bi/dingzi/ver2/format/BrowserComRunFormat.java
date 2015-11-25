/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BrowserComRunFormat.java 
 * @Package com.bi.dingzi.ver2.format 
 * @Description: 浏览器工具格式化
 * @author fuys
 * @date 2013-7-26 上午9:41:28 
 * @input:输入日志路径/2013-7-26  /dw/logs/tools/origin/BrowserComRun/$DIR_DATE
 * @output:输出日志路径/2013-7-26  /dw/logs/tools/result/ver2/BrowserComRun/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.format.BrowserComRunFormat --input $DIR_ORGINDATA_DAY_DTAIL_HOUR_INPUT --output /dw/logs/tools/result/ver2/BrowserComRun/$DIR_DAY --inpulzo 0
 * @inputFormat:PROTOCOL RPROTOCOL TIME IP CATEGORY NAME VERSION MAC GUID BRONAME BROVERSION SUC URL TYPE STRATERY
 * @ouputFormat:DATEID HOUR IP CATEGORY NAME VERSION MAC GUID BRONAME BROVERSION SUC URL TYPE STRATERY
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
 * @ClassName: BrowserComRunFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-7-26 上午9:41:28
 */
public class BrowserComRunFormat extends Configured implements Tool {

    enum BrowserComRunEnum {

        /**
         * 
         * "rprotocol", # 日志请求协议版本号，由前端发送，表明前端发送的版本号 "category", #
         * 浏览器组件的类别，0=ie-bho;1=ie-activex;2=ff-extension;3=ff-plugin "name", #
         * 组件名称 "version", # 组件版本 "mac", # 本机mac地址 "guid", #
         * 计算机计算出来的用户标识，Globally Unique Identifier（全球唯一标识符） "broname", # 浏览器名称
         * "broversion", # 浏览器版本 "suc", #
         * 是否启动或拉起成功，1表示启动成功，0表示启动失败，2表示拉起成功，3表示拉起失败 "url", #
         * 拉起风行客户端或钉子时浏览器的url（启动时为空） "type", # 0拉客户端，1拉钉子（启动时为空） "stratery", #
         * 拉起策略，1为需要拉起，0为不需要拉起（启动时为空）
         * 
         * 
         * 
         * 
         */
        PROTOCOL, RPROTOCOL, TIME, IP, CATEGORY, NAME, VERSION, MAC, GUID, BRONAME, BROVERSION, SUC, URL, TYPE, STRATERY;

    }

    public static class BrowserComRunFormatMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    DateFormatInfo.SEPARATOR, 0);
            // System.out.println("字段长度:" + fields.length);

            try {
                // if (fields.length > BrowserComRunEnum.STRATERY.ordinal()) {
                String timestampInfoStr = fields[BrowserComRunEnum.TIME
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                String ipStr = IPFormatUtil
                        .ipFormat(fields[BrowserComRunEnum.IP.ordinal()]);
                String categoryStr = fields[BrowserComRunEnum.CATEGORY
                        .ordinal()].toLowerCase();
                Long.parseLong(categoryStr);
                String nameStr = fields[BrowserComRunEnum.NAME.ordinal()];
                String versionStr = IPFormatUtil
                        .ipFormat(fields[BrowserComRunEnum.VERSION.ordinal()]);
                String macStr = fields[BrowserComRunEnum.MAC.ordinal()];
                String guidStr = fields[BrowserComRunEnum.GUID.ordinal()];

                String bronameStr = fields[BrowserComRunEnum.BRONAME.ordinal()]
                        .toLowerCase();

                String broversionStr = fields[BrowserComRunEnum.BROVERSION
                        .ordinal()];
                String sucStr = fields[BrowserComRunEnum.SUC.ordinal()];
                Long.parseLong(sucStr);
                String urlStr = fields[BrowserComRunEnum.URL.ordinal()];
                String typeStr = fields[BrowserComRunEnum.TYPE.ordinal()];
                // Long.parseLong(typeStr);
                // STRATERY
                String strateStr = fields[BrowserComRunEnum.STRATERY.ordinal()];
                // Long.parseLong(strateStr);
                // versionId
                long versionId = -0l;
                versionId = IPFormatUtil.ip2long(versionStr);
                String outValueStr = dateId + DateFormatInfo.SEPARATOR
                        + hourIdStr + DateFormatInfo.SEPARATOR + ipStr
                        + DateFormatInfo.SEPARATOR + categoryStr
                        + DateFormatInfo.SEPARATOR + nameStr
                        + DateFormatInfo.SEPARATOR + versionStr
                        + DateFormatInfo.SEPARATOR + macStr
                        + DateFormatInfo.SEPARATOR + guidStr
                        + DateFormatInfo.SEPARATOR + bronameStr
                        + DateFormatInfo.SEPARATOR + broversionStr
                        + DateFormatInfo.SEPARATOR + sucStr
                        + DateFormatInfo.SEPARATOR + urlStr
                        + DateFormatInfo.SEPARATOR + typeStr
                        + DateFormatInfo.SEPARATOR + strateStr
                        + DateFormatInfo.SEPARATOR + versionId;
                context.write(new Text(outValueStr), NullWritable.get());
                // }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

        }

    }

    public static class BrowserComRunFormatReducer extends
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
        commIsLZOArgs.init("browsercomrunformat.jar");
        commIsLZOArgs.parse(args);
        int res = ToolRunner.run(new Configuration(),
                new BrowserComRunFormat(), commIsLZOArgs.getCommsParam());
        System.out.println(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "5_product_dingzi_browsercomrunformat");
        job.setJarByClass(BrowserComRunFormat.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BrowserComRunFormatMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        if (null != args[2] && "1".equalsIgnoreCase(args[2].trim())) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
