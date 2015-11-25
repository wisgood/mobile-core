package com.bi.format.stuck;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.constant.DimensionConstant;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.common.logenum.StuckEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.FormatMobileUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: StuckFormat
 * @Description: Clean and transform stuck raw log data to generate basic data
 * @author wangxw
 * @date 2013-5-15
 */
public class StuckFormat extends Configured implements Tool {

    /**
     * @ClassName: StuckFormatMap
     * @Description: Static inner classes
     */
    public static class StuckFormatMaper extends
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

        public String formatLog(String originalLog) throws Exception {
            String[] fields = originalLog.split(",");
            fields = SpecialVersionRecomposeFormatMobileUtil
                    .recomposeBySpecialVersionIndex(fields,
                            StuckEnum.class.getName());

            String tmpstampInfoStr = fields[StuckEnum.TIMESTAMP.ordinal()];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(tmpstampInfoStr);
            // dataId
            String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
            String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

            // hourid
            int hourId = Integer.parseInt(hourIdStr);

            String platInfo = fields[StuckEnum.DEV.ordinal()];
            platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

            // platid
            int platId = 0;
            platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
            String versionInfo = fields[StuckEnum.VER.ordinal()];

            long versionId = 0l;
            versionId = IPFormatUtil.ip2long(versionInfo);

            // ipinfo
            String ipInfoStr = fields[StuckEnum.IP.ordinal()];
            long ipLong = 0;
            ipLong = IPFormatUtil.ip2long(ipInfoStr);

            java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                    .getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            // mac地址
            String macInfoStr = fields[StuckEnum.MAC.ordinal()];
            MACFormatUtil.isCorrectMac(macInfoStr);
            String macInfor = MACFormatUtil.macFormatToCorrectStr(macInfoStr);
            String ipFormatStr = IPFormatUtil.ipFormat(ipInfoStr);

            
            long serverId=0;
            serverId = IPFormatUtil.ip2long(fields[StuckEnum.SERVERIP
                    .ordinal()]);
            Map<ConstantEnum, String> serverdebugMap = dmServerdebugRuleDAO
                    .getDMOjb(serverId + "");
            serverId = Long.parseLong(serverdebugMap
                    .get(ConstantEnum.SERVER_ID));
            FormatMobileUtil.filerNoNumber(fields[StuckEnum.NT.ordinal()],
                    "NetWork type ");
            int netType = Integer.parseInt(fields[StuckEnum.NT.ordinal()]);
            long stuckTime = FormatMobileUtil.parseDoubleToLong(fields,
                    StuckEnum.class.getName(), StuckEnum.STUCKTIME.toString());
            StringBuilder formatLog = new StringBuilder();
            formatLog.append(dateId + "\t");
            formatLog.append(hourId + "\t");
            formatLog.append(platId + "\t");
            formatLog.append(versionId + "\t");
            formatLog.append(provinceId + "\t");
            formatLog.append(ispId + "\t");
            formatLog.append(DimensionConstant.TOTTAL_DEFAULT_VALUE + "\t");
            formatLog.append(netType + "\t");
            formatLog.append(fields[StuckEnum.BUFFEROK.ordinal()] + "\t");
            formatLog.append(stuckTime + "\t");
            formatLog.append(macInfor + "\t");
            formatLog.append(ipFormatStr);
            return formatLog.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originLog = value.toString();
            if (!lengthMeet(originLog.split(",").length)) {
                multipleOutputs.write(new Text("short"), new Text(originLog),
                        "_error/part");
                return;
            }
            String formatLog;
            try {
                formatLog = formatLog(originLog);
                context.write(new Text(formatLog), new Text(""));
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(e.getMessage()), new Text(
                        originLog), "_error/part");
                return;

            }
        }

        private boolean lengthMeet(int length) {

            return length > StuckEnum.STUCKTIME.ordinal();
        }
    }

    public static class DbufferFormatReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text());
            }

        }
    }

    /**
     * @ClassName: StuckFormatReduce
     * @Description: Static inner classes
     */
    public static class StuckFormatReducer extends
            Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text());
            }
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

        nRet = ToolRunner.run(new Configuration(), new StuckFormat(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "mobilequality-stuck-format");
        job.setJarByClass(StuckFormat.class);
        HdfsUtil.deleteDir(args[1]);
        for (Path path : HdfsUtil.listPaths(args[0])) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(StuckFormatMaper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
