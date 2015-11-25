/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ChuanglianQiDong.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 串联启动
 * @author wang
 * @date 2013-8-1 下午11:59:59 
 * @input:输入日志路径/2013-8-1 /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY
 * @output:输出日志路径/2013-8-1 /dw/logs/tools/result/ver2/chuanglianqidong/$DIR_DAY 
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.DingziQiDong  -i /dw/logs/tools/result/ver2/FsPlatformBoot/$DIR_DAY -o /dw/logs/tools/result/ver2/dingziqidong/$DIR_DAY  --files /disk6/datacenter/5_product/tools/config/DM_TOOLS_DZNAME 
 * @inputFormat:DATEID  HOURId  IP  ACTION   ACTIONRESULT    ACTIONOBJECTVER     CHANNEL     MAC     GUID    NAME    VERSION     ACTIONTIME      VERSIONID   ACTIONOBJECTVERID
 * @ouputFormat:DateId  VER_ID  DZ_QD_S_USER_NUM    DZ_QD_S_NUM     DZ_QD_USER_NUM
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

public class ChuanglianQiDong extends Configured implements Tool {

    private enum FsPlatformActionFormatEnum {
        DATEID, HOURId, IP, ACTION, ACTIONRESULT, ACTIONOBJECTVER, CHANNEL, MAC, GUID, NAME, VERSION, ACTIONTIME, VERSIONID, ACTIONOBJECTVERID

    }

    private static final String SEPARATOR = "\t";

    private static final int LENGTH = 14;

    private static class ChuangLianQiDongMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    DateFormatInfo.SEPARATOR, 0);
            // if (fields.length < LENGTH)
            // return;
            String date = fields[FsPlatformActionFormatEnum.DATEID.ordinal()];
            String outKey = date;
            String action = fields[FsPlatformActionFormatEnum.ACTION.ordinal()];
            if (!actionOk(action))
                return;
            String mac = fields[FsPlatformActionFormatEnum.MAC.ordinal()];
            String outValue = mac + SEPARATOR + action;
            context.write(new Text(outKey), new Text(outValue));

        }

        private boolean actionOk(String action) {
            if (null == action)
                return false;
            if ("8.screensaver".equals(action) || "9.lockscreen".equals(action))
                return true;
            return false;
        }
    }

    public static class ChuangLianQiDongReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> allMac = new HashSet<String>();
            Set<String> chuangLianMac = new HashSet<String>();
            Set<String> pingBaoMac = new HashSet<String>();

            for (Text value : values) {
                String[] fields = DateFormat.split(value.toString(),
                        DateFormatInfo.SEPARATOR, 0);
                String mac = fields[0];
                String action = fields[1];
                allMac.add(mac);
                if ("8.screensaver".equals(action))
                    pingBaoMac.add(mac);
                else if ("9.lockscreen".equals(action))
                    chuangLianMac.add(mac);

            }

            StringBuilder outputKey = new StringBuilder();
            outputKey.append(allMac.size() + SEPARATOR);
            outputKey.append(chuangLianMac.size() + SEPARATOR);
            outputKey.append(pingBaoMac.size());
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

        nRet = ToolRunner.run(new Configuration(), new ChuanglianQiDong(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "dingzi_chuanglianqidong");
        job.setJarByClass(ChuanglianQiDong.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ChuangLianQiDongMapper.class);
        job.setReducerClass(ChuangLianQiDongReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
