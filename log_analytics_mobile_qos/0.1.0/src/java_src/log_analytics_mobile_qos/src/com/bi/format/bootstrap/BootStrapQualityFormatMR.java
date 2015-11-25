/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: BootStrapQualityFormatMR.java 
 * @Package com.bi.format.bootstrap 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-29 下午4:26:11 
 * @input:输入日志路径/2013-8-29
 * @output:输出日志路径/2013-8-29
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.format.bootstrap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.constant.DimensionConstant;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.common.logenum.BootStrapEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.FormatMobileUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: BootStrapQualityFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-29 下午4:26:11
 */
public class BootStrapQualityFormatMR extends Configured implements Tool {

    public static class BootStrapQualityFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerdebugRuleDAO = null;

        private MultipleOutputs<Text, Text> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmServerdebugRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();
            dmPlatyRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_PLAT_PATH));
            dmIPRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_IPTABLE_PATH));
            dmServerdebugRuleDAO.parseDMObj(new File(
                    DimFilePath.CLUSTER_SERVER_PATH));
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }

        public String logFormat(String originLog) {
            String[] fields = DataFormatUtils.split(originLog,
                    DataFormatUtils.COMMA_SEPARATOR, 0);
            final String empty = "";

            try {
                fields = SpecialVersionRecomposeFormatMobileUtil
                        .recomposeBySpecialVersion(fields,
                                BootStrapEnum.class.getName());

                String timeStampOrigin = fields[BootStrapEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[BootStrapEnum.VER.ordinal()];
                String platOrigin = fields[BootStrapEnum.DEV.ordinal()];
                String macOrigin = fields[BootStrapEnum.MAC.ordinal()];
                String serverIpOrigin = "0.0.0.0";
                String ipOrigin = fields[BootStrapEnum.IP.ordinal()];
                String bootTypeOrigin = fields[BootStrapEnum.BTYPE.ordinal()];
                String bTimeOrigin = fields[BootStrapEnum.BTIME.ordinal()];
                String okTypeOrigin = fields[BootStrapEnum.OK.ordinal()];

                // dateid,hourid
                Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                        .formatTimestamp(timeStampOrigin);

                String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
                String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

                // versionId

                long versionId = 0l;
                versionId = IPFormatUtil.ip2long(versionOrigin);

                // platid

                platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
                int platId = 0;
                platId = dmPlatyRuleDAO.getDMOjb(platOrigin);

                // mac_format
                String macFormat = FormatMobileUtil.getMac(fields, platId,
                        versionId, BootStrapEnum.class.getName());

                // ip_format,city_id,isp_id
                String ipFormat = IPFormatUtil.ipFormat(ipOrigin);
                long ip = 0;
                ip = IPFormatUtil.ip2long(ipOrigin);
                Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ip);
                String provenceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // server_id
                long serverId = 0;
                serverId = IPFormatUtil.ip2long(serverIpOrigin);
                Map<ConstantEnum, String> serverInfoMap = dmServerdebugRuleDAO
                        .getDMOjb(serverId + "");
                serverId = Long.parseLong(serverInfoMap
                        .get(ConstantEnum.SERVER_ID));
                // btime
                int bTimeFormat = processBtime(bTimeOrigin);
                if (bTimeFormat < 0) {
                    throw new Exception("btime cannot be a negative");
                }

                DataFormatUtils
                        .filerNoNumber(fields[BootStrapEnum.NT.ordinal()]);
                // DATE_ID, HOUR_ID, PLAT_ID, VERSION_ID, PROVINCE_ID, ISP_ID,
                // SERVER_ID, NET_TYPE, MAC_FORMAT, IP_FORMAT, OK,
                // BTM_FORMAT,BTYPE_FORMAT
                StringBuilder formatLog = new StringBuilder();
                formatLog.append(dateId);
                formatLog.append("\t");
                formatLog.append(hourId);
                formatLog.append("\t");
                formatLog.append(platId);
                formatLog.append("\t");
                formatLog.append(versionId);
                formatLog.append("\t");
                formatLog.append(provenceId);
                formatLog.append("\t");
                formatLog.append(ispId);
                formatLog.append("\t");
                formatLog.append(DimensionConstant.TOTTAL_DEFAULT_VALUE);
                formatLog.append("\t");
                formatLog.append(fields[BootStrapEnum.NT.ordinal()]);
                formatLog.append("\t");
                formatLog.append(macFormat);
                formatLog.append("\t");
                formatLog.append(ipFormat);
                formatLog.append("\t");
                formatLog.append(okTypeOrigin);
                formatLog.append("\t");
                formatLog.append(bTimeFormat);
                formatLog.append("\t");
                formatLog.append(bootTypeOrigin);
                return formatLog.toString();

            }
            catch(Exception e) {
                // TODO Auto-generated catch block

                return empty;
            }

        }

        private int processBtime(String origin) {
            double result = Double.valueOf(origin);
            return (int) result;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originLog = value.toString();
            if (!lengthMeet(DataFormatUtils.split(originLog,
                    DataFormatUtils.COMMA_SEPARATOR, 0).length)) {
                multipleOutputs.write(new Text("short"), new Text(originLog),
                        "_error/part");
                return;
            }
            String formatLog = null;
            try {
                formatLog = logFormat(originLog);
                if (!"".equalsIgnoreCase(formatLog)) {
                    context.write(new Text(formatLog), new Text(""));
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(e.getMessage()), new Text(
                        originLog), "_error/part");
                return;

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

        private boolean lengthMeet(int length) {

            return length >= BootStrapEnum.OK.ordinal() + 1;
        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int res = ToolRunner.run(new Configuration(),
                new BootStrapQualityFormatMR(), args);
        System.out.println(res);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        for (int i = 0; i < args.length; i++) {

            System.out.println(i + ":" + args[i]);
        }
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(BootStrapQualityFormatMR.class);
        job.setMapperClass(BootStrapQualityFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_bootStrapQualityFormat_" + executeDateStr);
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
