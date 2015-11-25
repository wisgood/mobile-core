package com.bi.mobilequality.bootstrap.format.errordata;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.mobile.comm.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.mobilequality.bootstrap.format.dataenum.BootStrapEnum;

public class BootStrapFormatErrorMR {
    public static class BootStrapFormatErrorMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(BootStrapFormatErrorMapper.class.getName());

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerInfoRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmServerInfoRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();

            if (isLocalRunMode(context)) {
                logger.info("rummode is local!");
                String dmMobilePlayFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
                dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));
                String dmQuodaoFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name());
                dmQuDaoRuleDAO.parseDMObj(new File(dmQuodaoFilePath));
                String dmIpTableFilePath = context.getConfiguration().get(
                        ConstantEnum.IPTABLE_FILEPATH.name());
                dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
                String dmServerFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name());
                dmServerInfoRuleDAO.parseDMObj(new File(dmServerFilePath));
            }
            else {
                logger.info("BootStrapETLMap rummode is cluster!");
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
                dmQuDaoRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_QUDAO
                        .name().toLowerCase()));
                dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                        .toLowerCase()));
                dmServerInfoRuleDAO.parseDMObj(new File(
                        ConstantEnum.DM_MOBILE_SERVER.name().toLowerCase()));
            }
        }

        private boolean isLocalRunMode(Context context) {

            String mapredJobTrackerMode = context.getConfiguration().get(
                    "mapred.job.tracker");
            if (null != mapredJobTrackerMode
                    && ConstantEnum.LOCAL.name().equalsIgnoreCase(
                            mapredJobTrackerMode)) {
                return true;
            }
            return false;
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
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
                MACFormatUtil.isCorrectMac(macOrigin);
                String macFormat = MACFormatUtil
                        .macFormatToCorrectStr(macOrigin);

                // ip_format,city_id,isp_id
                String ipFormat = IPFormatUtil.ipFormat(ipOrigin);
                long ip = 0;
                ip = IPFormatUtil.ip2long(ipOrigin);
                Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ip);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // server_id
                long serverId = 0;
                serverId = IPFormatUtil.ip2long(serverIpOrigin);
                Map<ConstantEnum, String> serverInfoMap = dmServerInfoRuleDAO
                        .getDMOjb(serverId + "");
                serverId = Long.parseLong(serverInfoMap
                        .get(ConstantEnum.SERVER_ID));
                // btime
                int bTimeFormat = processBtime(bTimeOrigin);
                if (bTimeFormat < 0) {
                    throw new Exception("btime cannot be a negative");
                }

                return empty;

            }
            catch(Exception e) {
                return e.getMessage() + "\t" + originLog;
            }

        }

        private int processBtime(String origin) {
            double result = Double.valueOf(origin);
            return (int) result;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originLog = value.toString().replaceAll(",", "\t");
            if (lengthMeet(originLog.split("\t").length)) {
                String formatLog = logFormat(originLog);
                if (null != formatLog && !("".equalsIgnoreCase(formatLog))) {
                    context.write(new Text(formatLog.split("\t")[0]), new Text(
                            formatLog));
                }
            }
            else {
                context.write(new Text("column lengh is short"), new Text(
                        originLog));
                return;
            }

        }

        private boolean lengthMeet(int length) {

            return length >= BootStrapEnum.OK.ordinal() + 1;
        }

    }

    public static class BootStrapFormatErrorReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }

        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(BootStrapFormatErrorMR.class);
        job.setJobName("BootStrapFormatError-MobileServiceQuality");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        // 设置配置文件默认路径
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name(),
                "conf/dm_mobile_qudao");
        job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
                "conf/ip_table");
        job.getConfiguration().set(
                ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name(),
                "conf/dm_common_infohash");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name(),
                "conf/dm_mobile_server");
        FileInputFormat.addInputPath(job, new Path("temp"));
        FileOutputFormat.setOutputPath(job, new Path(
                "mq_output_bootstrap_error_5"));
        job.setMapperClass(BootStrapFormatErrorMapper.class);
        job.setReducerClass(BootStrapFormatErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
