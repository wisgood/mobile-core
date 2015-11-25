/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DingziZuoGong.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 钉子基础指标钉子做功计算
 * @author wang
 * @date 2013-8-1 下午11:59:59 
 * @input:输入日志路径/2013-8-1 /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY
 * @output:输出日志路径/2013-8-1 /dw/logs/tools/result/ver2/dingzizuogong/$DIR_DAY
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.DingziZuoGong -i /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY -o /dw/logs/tools/result/ver2/dingzizuogong/$DIR_DAY
 * @inputFormat: DATEID  HOURId  IP  ACTION  ACTIONRESULT  ACTIONOBJECTVER  CHANNEL  MAC  GUID  NAME  VERSION  ACTIONTIME VERSIONID  ACTIONOBJECTVERID
 * @ouputFormat:DateId VER_ID   DZ_LAQI_S_USER_NUM   DZ_ LAQI_S_NUM     DZ_ LAQI_USER_NUM   DZ_ LAQI_TBS_USER_NUM   DZ_ LAQI_TBS _NUM   DZ_LAQI_TB_USER_NUM     DZ_LAQI_ATBS_USER_NUM  DZ_LAQI_ITBS_USER_NUM    DZ_LAQI_BS_USER_NUM     DZ_LAQI_BS _NUM     DZ_LAQI_BT _USER_NUM
 */
package com.bi.dingzi.ver2.compute;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;

/**
 * @ClassName: DingziQiDong
 * @Description: 这里用一句话描述这个类的作用
 * @author wang
 * @date 2013-7-26 上午1:02:06
 */
public class DingziZuoGong extends Configured implements Tool {

    private enum FsPlatformActionFormatEnum {
        DATEID, HOURId, IP, ACTION, ACTIONRESULT, ACTIONOBJECTVER, CHANNEL, MAC, GUID, NAME, VERSION, ACTIONTIME, VERSIONID, ACTIONOBJECTVERID

    }

    private static final String SEPARATOR = "\t";

    // private static final int LENGTH = 14;

    public static class DingziZuoGongMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    DateFormatInfo.SEPARATOR, 0);
            // if (fields.length < LENGTH)
            // return;

            // data
            String date = fields[FsPlatformActionFormatEnum.DATEID.ordinal()];
            ;
            String outKey = date + SEPARATOR
                    + fields[FsPlatformActionFormatEnum.VERSIONID.ordinal()];
            String actionResult = fields[FsPlatformActionFormatEnum.ACTIONRESULT
                    .ordinal()];
            if (!actionResultOk(actionResult))
                return;
            String mac = fields[FsPlatformActionFormatEnum.MAC.ordinal()];
            String outValue = mac + SEPARATOR + actionResult;
            context.write(new Text(outKey), new Text(outValue));

        }

        private boolean actionResultOk(String actionResult) {
            try {
                int result = Integer.parseInt(actionResult);
                return result == 10 || result == 11 || result == 20
                        || result == 21 || result == 22 || result == 23
                        || result == 30 || result == 31;
            }
            catch(Exception e) {
                return false;
            }
        }

    }

    public static class DingziZuoGongReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> pullClientSucMac = new HashSet<String>();
            Set<String> pullClientMac = new HashSet<String>();
            Set<String> pullSyntoolSucMac = new HashSet<String>();
            Set<String> pullSyntoolMac = new HashSet<String>();
            Set<String> pullAndriodSyntoolSucMac = new HashSet<String>();
            Set<String> pullIosSyntoolSucMac = new HashSet<String>();
            Set<String> pullGreenClientSucMac = new HashSet<String>();
            Set<String> pullGreenClientMac = new HashSet<String>();
            int pullClientSucRecords = 0;
            int pullSyntoolSucRecords = 0;
            int pullGreenClientSucRecords = 0;

            for (Text value : values) {
                String[] fields = DateFormat.split(value.toString(),
                        DateFormatInfo.SEPARATOR, 0);
                String mac = fields[0];
                int actionResult = Integer.parseInt(fields[1]);
                switch (actionResult) {
                case 10:
                    pullClientMac.add(mac);
                    break;
                case 11:
                    pullClientSucMac.add(mac);
                    pullClientSucRecords++;
                    pullClientMac.add(mac);
                    break;
                case 20:
                    pullSyntoolMac.add(mac);
                    break;
                case 21:
                    pullAndriodSyntoolSucMac.add(mac);
                    pullSyntoolSucMac.add(mac);
                    pullSyntoolMac.add(mac);
                    pullSyntoolSucRecords++;
                    break;
                case 22:
                    pullSyntoolMac.add(mac);
                    break;
                case 23:
                    pullIosSyntoolSucMac.add(mac);
                    pullSyntoolSucMac.add(mac);
                    pullSyntoolMac.add(mac);
                    pullSyntoolSucRecords++;

                    break;
                case 30:
                    pullGreenClientMac.add(mac);
                    break;
                case 31:
                    pullGreenClientMac.add(mac);
                    pullGreenClientSucMac.add(mac);
                    pullGreenClientSucRecords++;
                    break;

                default:
                    break;
                }

            }

            StringBuilder outputKey = new StringBuilder();
            outputKey.append(pullClientSucMac.size() + SEPARATOR);
            outputKey.append(pullClientSucRecords + SEPARATOR);
            outputKey.append(pullClientMac.size() + SEPARATOR);
            outputKey.append(pullSyntoolSucMac.size() + SEPARATOR);
            outputKey.append(pullSyntoolSucRecords + SEPARATOR);
            outputKey.append(pullSyntoolMac.size() + SEPARATOR);
            outputKey.append(pullAndriodSyntoolSucMac.size() + SEPARATOR);
            outputKey.append(pullIosSyntoolSucMac.size() + SEPARATOR);
            outputKey.append(pullGreenClientSucMac.size() + SEPARATOR);
            outputKey.append(pullGreenClientSucRecords + SEPARATOR);
            outputKey.append(pullGreenClientMac.size());
            context.write(key, new Text(outputKey.toString()));

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new DingziZuoGong(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "dingzi_dingzizuogong");
        job.setJarByClass(DingziZuoGong.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DingziZuoGongMapper.class);
        job.setReducerClass(DingziZuoGongReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
