package com.bi.mobile.bootstrap.format.errordata;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

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
import com.bi.mobile.bootstrap.format.dataenum.BootStrapEnum;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.comm.util.SpecialVersionRecomposeFormatMobileUtil;

public class BootStrapFormatFilterByErrorMR {
    public static class BootStrapFormatFilterByErrorMap extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(BootStrapFormatFilterByErrorMap.class.getName());

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

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

        public Map<String, String> getBootStrapFormatMap(String originalData) {
            Map<String, String> bootStrapETLMap = new WeakHashMap<String, String>();
            String[] splitSts = originalData.split(",");
            if (splitSts.length <= BootStrapEnum.FDISK.ordinal()) {
                bootStrapETLMap.put("colcum lenght less "
                        + (BootStrapEnum.FDISK.ordinal() + 1), originalData);

            }
            else {
                try {
                    splitSts = SpecialVersionRecomposeFormatMobileUtil
                            .recomposeBySpecialVersion(splitSts,
                                    BootStrapEnum.class.getName());
                    String timestampInfoStr = splitSts[BootStrapEnum.TIMESTAMP
                            .ordinal()];
                    java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                            .formatTimestamp(timestampInfoStr);
                    // dataId
                    String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                    String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

                    // hourid
                    int hourId = Integer.parseInt(hourIdStr);
                    logger.info("dateId:" + dateId);
                    logger.info("hourId:" + hourId);

                    // 获取设备类型
                    String platInfo = splitSts[BootStrapEnum.DEV.ordinal()];
                    platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

                    // platid
                    int platId = 0;
                    platId = dmPlatyRuleDAO.getDMOjb(platInfo);
                    logger.info("platId:" + platId);
                    String versionInfo = splitSts[BootStrapEnum.VER.ordinal()];

                    // versionId
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    logger.info("versionId:" + versionId);

                    // qudaoId
                    int qudaoId = SidFormatMobileUtil.getSidByEnum(splitSts,
                            dmQuDaoRuleDAO, BootStrapEnum.class.getName());
                    logger.info("qudaoInfo:" + qudaoId);

                    // bootType
                    String bootType = splitSts[BootStrapEnum.BTYPE.ordinal()];
                    int boottype = Integer.parseInt(bootType);
                    logger.info("bootType:" + bootType);

                    // ipinfo
                    String ipInfoStr = splitSts[BootStrapEnum.IP.ordinal()];
                    long ipLong = 0;
                    ipLong = IPFormatUtil.ip2long(ipInfoStr);
                    java.util.Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO
                            .getDMOjb(ipLong);
                    String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                    String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                    String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                    logger.info("ipinfo:" + ipRuleMap);
                    // mac地址
                    String macInfoStr = splitSts[BootStrapEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macInfoStr);
                    MACFormatUtil.isCorrectMac(macInfoStr);
                    String macInfor = MACFormatUtil
                            .macFormatToCorrectStr(macInfoStr);

                    // process ok_type
                    String okType = splitSts[BootStrapEnum.OK.ordinal()];
                    // process version_str
                    String versionStr = FormatMobileUtil
                            .versionFormatMobileUtil(versionInfo);

                }
                catch(Exception e) {

                    bootStrapETLMap.put(e.getMessage(), originalData);
                }
            }

            return bootStrapETLMap;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, String> bootStrapETLMap = getBootStrapFormatMap(line);
            if (null != bootStrapETLMap && bootStrapETLMap.size() > 0) {
                Set<Map.Entry<String, String>> set = bootStrapETLMap.entrySet();
                for (Iterator<Map.Entry<String, String>> it = set.iterator(); it
                        .hasNext();) {
                    Map.Entry<String, String> entry = (Map.Entry<String, String>) it
                            .next();
                    context.write(new Text(entry.getKey()),
                            new Text(entry.getValue()));
                }

            }
        }
    }

    public static class BootStrapFormatFilterByErrorReduce extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(BootStrapFormatFilterByErrorMR.class);
        job.setJobName("BootStrapFormatFilterByError");
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
        FileInputFormat.addInputPath(job, new Path("input_bootsrap"));
        FileOutputFormat.setOutputPath(job,
                new Path("output_validate_bootsrap"));
        job.setMapperClass(BootStrapFormatFilterByErrorMap.class);
        job.setReducerClass(BootStrapFormatFilterByErrorReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
