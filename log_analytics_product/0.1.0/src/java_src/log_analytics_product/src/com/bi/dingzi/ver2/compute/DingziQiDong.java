/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DingziQiDong.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 钉子基础指标钉子启动计算
 * @author wang
 * @date 2013-8-1 下午11:59:59 
 * @input:输入日志路径/2013-8-1 /dw/logs/tools/result/ver2/FsPlatformBoot/$DIR_DAY
 * @output:输出日志路径/2013-8-1 /dw/logs/tools/result/ver2/dingziqidong/$DIR_DAY 
 * @executeCmd:hadoop jar log_analytics_product.jar com.bi.dingzi.ver2.compute.DingziQiDong  -i /dw/logs/tools/result/ver2/FsPlatformBoot/$DIR_DAY -o /dw/logs/tools/result/ver2/dingziqidong/$DIR_DAY  --files /disk6/datacenter/5_product/tools/config/DM_TOOLS_DZNAME 
 * @inputFormat:DATEID  HOURId  IP  ACTION   ACTIONRESULT    ACTIONOBJECTVER     CHANNEL     MAC     GUID    NAME    VERSION     ACTIONTIME      VERSIONID   ACTIONOBJECTVERID
 * @ouputFormat:DateId  VER_ID  NAME_ID     QDM_ID  DZ_QD_USER_NUM  DZ_QD_NUM  CL_USER_NUM    NCL_USER_NUM  YCL_NQD_USER_NUM  YCL_QD_USER_NUM  HAVE_ANDRIOD_SYNC_TOOL_USERS    
 */
package com.bi.dingzi.ver2.compute;

import java.io.File;
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
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;
import com.bi.dingzi.dimensionprocess.AbstractDimProcess;
import com.bi.dingzi.dimensionprocess.CommonDimProcess;

/**
 * @ClassName: DingziQiDong
 * @Description: 这里用一句话描述这个类的作用
 * @author wang
 * @date 2013-7-26 上午1:02:06
 */
public class DingziQiDong extends Configured implements Tool {

    private enum FsPlatFormBootFormatEnum {
        DATEID, HOURID, IP, BOOTMETHOD, CLIENTSTATE, SYNCTOOLSTATE, MAC, GUID, NAME, VERSION, OS, VERSIONID
    }

    private static final String SEPARATOR = "\t";

    private static final int LENGTH = 12;

    public static class DingziQiDongMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDimProcess<String, String> nameDimProcess = new CommonDimProcess<String, String>();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            if (isLocalRunMode(context)) {
                nameDimProcess.parseDimensionFile(new File(
                        "config/DM_TOOLS_DZNAME"));
            }
            else {
                File file = new File("DM_TOOLS_DZNAME");
                nameDimProcess.parseDimensionFile(file);
            }
        }

        private boolean isLocalRunMode(Context context) {

            String mapredJobTrackerMode = context.getConfiguration().get(
                    "mapred.job.tracker");
            if (null != mapredJobTrackerMode
                    && "local".equalsIgnoreCase(mapredJobTrackerMode)) {
                return true;
            }
            return false;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueStr = value.toString();
            String[] fields = DateFormat.split(valueStr,
                    DateFormatInfo.SEPARATOR, 0);
            // if (fields.length < LENGTH)
            // return;
            // data
            String date = fields[FsPlatFormBootFormatEnum.DATEID.ordinal()];
            String nameId = null;
            try {
                nameId = nameDimProcess
                        .getDimensionId(fields[FsPlatFormBootFormatEnum.NAME
                                .ordinal()]);

                // process log bug :name is null
                // if (null == nameId)
                // return;
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            String outKey = date + SEPARATOR
                    + fields[FsPlatFormBootFormatEnum.VERSIONID.ordinal()]
                    + SEPARATOR + nameId + SEPARATOR
                    + fields[FsPlatFormBootFormatEnum.BOOTMETHOD.ordinal()];
            String clientState = fields[FsPlatFormBootFormatEnum.CLIENTSTATE
                    .ordinal()];
            String synctoolState = fields[FsPlatFormBootFormatEnum.SYNCTOOLSTATE
                    .ordinal()];
            String mac = fields[FsPlatFormBootFormatEnum.MAC.ordinal()];
            String outValue = mac + SEPARATOR + clientState + SEPARATOR
                    + synctoolState;
            context.write(new Text(outKey), new Text(outValue));

        }

    }

    public static class DingziQiDongReducer extends
            Reducer<Text, Text, Text, Text> {

        private long bootRecords;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> bootUserMac = new HashSet<String>();
            Set<String> haveClientMac = new HashSet<String>();
            Set<String> haveClientMacBoot = new HashSet<String>();
            Set<String> haveClientMacNoBoot = new HashSet<String>();
            Set<String> noClientMac = new HashSet<String>();
            Set<String> haveSynctoolMac = new HashSet<String>();

            for (Text value : values) {
                bootRecords++;
                String[] fields = DateFormat.split(value.toString(),
                        DateFormatInfo.SEPARATOR, 0);
                String mac = fields[0];
                bootUserMac.add(mac);
                try {
                    int clientState = Integer.parseInt(fields[1]);
                    int synctoolState = Integer.parseInt(fields[2]);

                    // process success
                    if (0 == clientState) {
                        noClientMac.add(mac);
                    }
                    else if (1 == clientState) {
                        haveClientMac.add(mac);
                        haveClientMacNoBoot.add(mac);
                    }
                    else if (2 == clientState) {
                        haveClientMac.add(mac);
                        haveClientMacBoot.add(mac);
                    }
                    // process pull client
                    if (1 == synctoolState) {
                        haveSynctoolMac.add(mac);
                    }

                }
                catch(Exception e) {
                    continue;
                }

            }
            StringBuilder outputKey = new StringBuilder();
            outputKey.append(bootUserMac.size() + SEPARATOR);
            outputKey.append(bootRecords + SEPARATOR);
            outputKey.append(noClientMac.size() + SEPARATOR);
            outputKey.append(haveClientMacNoBoot.size() + SEPARATOR);
            outputKey.append(haveClientMacBoot.size() + SEPARATOR);
            outputKey.append(haveClientMac.size() + SEPARATOR);
            outputKey.append(haveSynctoolMac.size());
            context.write(key, new Text(outputKey.toString()));

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new ConfigFileCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new DingziQiDong(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "dingzi_qidong");
        job.setJarByClass(DingziQiDong.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DingziQiDongMapper.class);
        job.setReducerClass(DingziQiDongReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
