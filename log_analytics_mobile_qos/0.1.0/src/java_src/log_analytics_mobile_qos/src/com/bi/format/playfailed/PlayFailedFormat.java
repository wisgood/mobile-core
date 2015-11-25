package com.bi.format.playfailed;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.common.logenum.PlayFailEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.FormatMobileUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class PlayFailedFormat extends Configured implements Tool {

    public static class PlayFailedFormatMapper extends
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

        private String formatLog(String originLog) throws Exception {
            String[] fields = originLog.split(",");
            String timeStampOrigin = fields[PlayFailEnum.TIMESTAMP.ordinal()];
            String versionOrigin = fields[PlayFailEnum.VER.ordinal()];
            String platOrigin = fields[PlayFailEnum.DEV.ordinal()];
            String macOrigin = fields[PlayFailEnum.MAC.ordinal()];
            String ipOrigin = fields[PlayFailEnum.IP.ordinal()];
            int errorOrigin = Integer.parseInt(fields[PlayFailEnum.ERROR
                    .ordinal()]);
            // data_id ,hour_id

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
            MACFormatUtil.isCorrectMac(macOrigin);
            String macFormat = MACFormatUtil.macFormatToCorrectStr(macOrigin);
            if (macFormat.equals(""))
                System.out.println(originLog);

            MACFormatUtil.isCorrectMac(macOrigin);
            macFormat = FormatMobileUtil.getMac(fields, platId, versionId,
                    PlayFailEnum.class.getName());
            // ip_format,city_id,isp_id
            String ipFormat = IPFormatUtil.ipFormat(ipOrigin);
            long ip = 0;
            ip = IPFormatUtil.ip2long(ipOrigin);
            Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ip);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

            // server_id
            long serverId = 0;
            serverId = IPFormatUtil.ip2long(fields[PlayFailEnum.SERVERIP
                    .ordinal()]);
            Map<ConstantEnum, String> serverdebugMap = dmServerdebugRuleDAO
                    .getDMOjb(serverId + "");
            serverId = Long.parseLong(serverdebugMap
                    .get(ConstantEnum.SERVER_ID));
            FormatMobileUtil.filerNoNumber(fields[PlayFailEnum.NT.ordinal()],
                    "NetWork type ");
            int netTypeId = Integer.parseInt(fields[PlayFailEnum.NT.ordinal()]);
            StringBuilder formatLog = new StringBuilder();
            formatLog.append(dateId);
            formatLog.append("\t");
            formatLog.append(hourId);
            formatLog.append("\t");
            formatLog.append(platId);
            formatLog.append("\t");
            formatLog.append(versionId);
            formatLog.append("\t");
            formatLog.append(provinceId);
            formatLog.append("\t");
            formatLog.append(ispId);
            formatLog.append("\t");
            formatLog.append(serverId);
            formatLog.append("\t");
            formatLog.append(netTypeId);
            formatLog.append("\t");
            formatLog.append(macFormat);
            formatLog.append("\t");
            formatLog.append(ipFormat);

            return formatLog.toString();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {
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

            return length >= 5;
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

        nRet = ToolRunner.run(new Configuration(), new PlayFailedFormat(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "mobilequality-playfailed-format");
        job.setJarByClass(PlayFailedFormat.class);
        HdfsUtil.deleteDir(args[1]);
        for (Path path : HdfsUtil.listPaths(args[0])) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PlayFailedFormatMapper.class);
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
