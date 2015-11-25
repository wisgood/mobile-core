/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ChuanglianZuoGong.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 窗帘做功
 * @author wang
 * @date 2013-8-1 下午11:59:59 
 * @input:输入日志路径/2013-8-1 /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY
 * @output:输出日志路径/2013-8-1 /dw/logs/tools/result/ver2/chuanglianzuogong/$DIR_DAY 
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.ChuanglianZuoGong --input /dw/logs/tools/result/ver2/FsPlatformAction/$DIR_DAY  --output /dw/logs/tools/result/ver2/chuanglianzuogong/$DIR_DAY 
 * @inputFormat: DATEID  HOURId  IP  ACTION  ACTIONRESULT  ACTIONOBJECTVER  CHANNEL  MAC  GUID  NAME  VERSION  ACTIONTIME VERSIONID  ACTIONOBJECTVERID
 * @ouputFormat:DateId CLT _ID   VERSION_ID   DZ_LQ_USER_NUM  DZ_LQ _NUM   DZ_LA_S_USER_NUM   DZ_LA_S _NUM  DZ_LA_F_USER_NUM     DZ_NLA_USER_NUM  DZ_Y_NLA_USER_NUM    DZ_Q_NLA_USER_NUM     DZ_LAD_ USER_NUM     DZ_LAD_NUM  DZ_DD_S_ USER_NUM  DZ_DD_S _NUM  DZ_DD_F_ USER_NUM  DZ_DD_F _NUM  DZ_LDD_USER _NUM  DZ_ND_USER _NUM
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
 * @ClassName: ChuanglianZuoGong
 * @Description: 这里用一句话描述这个类的作用
 * @author wang
 * @date 2013-8-1 下午11:59:59
 */
public class ChuanglianZuoGong extends Configured implements Tool {

    private enum FsPlatformActionFormatEnum {
        DATEID, HOURId, IP, ACTION, ACTIONRESULT, ACTIONOBJECTVER, CHANNEL, MAC, GUID, NAME, VERSION, ACTIONTIME, VERSIONID, ACTIONOBJECTVERID

    }

    private static final String SEPARATOR = "\t";

    private static final int LENGTH = 14;

    public static class ChuanglianZuoGongMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueStr = value.toString();
            String[] fields =  DateFormat.split(valueStr, DateFormatInfo.SEPARATOR, 0);
            if ((fields.length < LENGTH))
                return;
            String date = fields[FsPlatformActionFormatEnum.DATEID.ordinal()];
            String action = fields[FsPlatformActionFormatEnum.ACTION.ordinal()];
            String category = getCatagory(action);
            if (null == category)
                return;

            String outKey = date + SEPARATOR + category + SEPARATOR
                    + fields[FsPlatformActionFormatEnum.ACTIONOBJECTVERID.ordinal()];
            String mac = fields[FsPlatformActionFormatEnum.MAC.ordinal()];
            String actionResult = fields[FsPlatformActionFormatEnum.ACTIONRESULT
                    .ordinal()];
            String outValue = mac + SEPARATOR + actionResult + SEPARATOR
                    + action;
            context.write(new Text(outKey), new Text(outValue));

        }

        private String getCatagory(String action) {
            if (action.contains("screensaver"))
                return "1";
            else if (action.contains("lockscreen"))
                return "2";
            else
                return null;

        }

    }

    public static class ChuanglianZuoGongReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // client
            long pullClientRecords = 0;
            Set<String> pullClientUsersMac = new HashSet<String>();
            Set<String> pullClientSucUsersMac = new HashSet<String>();
            long pullClientSucRecords = 0;
            Set<String> pullClientFailUsersMac = new HashSet<String>();
            Set<String> noPullByStrategyUsersMac = new HashSet<String>();
            Set<String> noPullByHaveUsersMac = new HashSet<String>();
            Set<String> noPullByExitUsersMac = new HashSet<String>();
            // dloader
            long pullDloaderRecords = 0;
            Set<String> pullDloaderUsersMac = new HashSet<String>();
            Set<String> downDloaderSucUsersMac = new HashSet<String>();
            long downDloaderSucRecords = 0;
            Set<String> downDloaderFailUsersMac = new HashSet<String>();
            long pullDloaderFailRecords = 0;
            Set<String> localInvokeDloaderUsersMac = new HashSet<String>();
            Set<String> noDloaderByStrategyUsersMac = new HashSet<String>();
            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                String mac = fields[0];
                String action = fields[2];
                if (action.contains("4.screensaverpullclient")
                        || action.contains("6.lockscreenpullupclient")) {
                    pullClientRecords++;
                    pullClientUsersMac.add(mac);
                }
                else if (action.contains("5.screensaverpulldloader")
                        || action.contains("7.lockScreenpullupdloader")) {
                    pullDloaderRecords++;
                    pullDloaderUsersMac.add(mac);

                }

                String actionResult = fields[1];

                if ("40".equals(actionResult) || "60".equals(actionResult)) {
                    pullClientFailUsersMac.add(mac);

                }
                else if ("41".equals(actionResult) || "61".equals(actionResult)) {
                    pullClientSucUsersMac.add(mac);
                    pullClientSucRecords++;
                }
                else if ("42".equals(actionResult) || "62".equals(actionResult)) {
                    noPullByStrategyUsersMac.add(mac);
                }
                else if ("43".equals(actionResult) || "63".equals(actionResult)) {
                    noPullByHaveUsersMac.add(mac);

                }
                else if ("44".equals(actionResult) || "64".equals(actionResult)) {
                    noPullByExitUsersMac.add(mac);

                }
                else if ("50".equals(actionResult) || "70".equals(actionResult)) {
                    downDloaderFailUsersMac.add(mac);
                }
                else if ("51".equals(actionResult) || "71".equals(actionResult)) {
                    downDloaderSucRecords++;
                    downDloaderSucUsersMac.add(mac);
                }
                else if ("52".equals(actionResult) || "72".equals(actionResult)) {
                    localInvokeDloaderUsersMac.add(mac);
                }
                else if ("53".equals(actionResult) || "73".equals(actionResult)) {
                    noDloaderByStrategyUsersMac.add(mac);

                }
            }

            StringBuilder outputKey = new StringBuilder();
            outputKey.append(pullClientUsersMac.size() + SEPARATOR);
            outputKey.append(pullClientRecords + SEPARATOR);
            outputKey.append(pullClientSucUsersMac.size() + SEPARATOR);
            outputKey.append(pullClientSucRecords + SEPARATOR);
            outputKey.append(pullClientFailUsersMac.size() + SEPARATOR);
            outputKey.append(noPullByStrategyUsersMac.size() + SEPARATOR);
            outputKey.append(noPullByHaveUsersMac.size() + SEPARATOR);
            outputKey.append(noPullByExitUsersMac.size() + SEPARATOR);
            outputKey.append(pullDloaderUsersMac.size() + SEPARATOR);
            outputKey.append(pullDloaderRecords + SEPARATOR);
            outputKey.append(downDloaderSucUsersMac.size() + SEPARATOR);
            outputKey.append(downDloaderSucRecords + SEPARATOR);
            outputKey.append(downDloaderFailUsersMac.size() + SEPARATOR);
            outputKey.append(pullDloaderFailRecords + SEPARATOR);
            outputKey.append(localInvokeDloaderUsersMac.size() + SEPARATOR);
            outputKey.append(noDloaderByStrategyUsersMac.size());
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

        nRet = ToolRunner.run(new Configuration(), new ChuanglianZuoGong(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "dingzi_chuanlianzuogong");
        job.setJarByClass(ChuanglianZuoGong.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ChuanglianZuoGongMapper.class);
        job.setReducerClass(ChuanglianZuoGongReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
