package com.bi.mobile.pushreach.format.errordata;

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
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.pushreach.format.dataenum.PushReachFormatEnum;
import com.bi.mobile.pushreach.format.dataenum.PushReachEnum;

public class PushReachFormatFilterByErrorMR {
    public static class PushReachFormatFilterByErrorMap extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private static Logger logger = Logger
                .getLogger(PushReachFormatFilterByErrorMap.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            if (this.isLocalRunMode(context)) {
                String dmMobilePlayFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
                this.dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));
                String dmQuodaoFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name());
                this.dmQuDaoRuleDAO.parseDMObj(new File(dmQuodaoFilePath));
                String dmIpTableFilePath = context.getConfiguration().get(
                        ConstantEnum.IPTABLE_FILEPATH.name());
                this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));
            }
            else {

                logger.info("PushReachETLMR rummode is cluster!");
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
                this.dmQuDaoRuleDAO.parseDMObj(new File(
                        ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
                this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
                        .name().toLowerCase()));
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

        public Map<String, String> getPushReachFormatMap(String originalData) {
            // PushReachETL pushReachETL = null;
            // StringBuilder pushReachETLSB = new StringBuilder();
            Map<String, String> pushReachETLMap = new WeakHashMap<String, String>();
            String[] splitSts = originalData.split(",");
            if (splitSts.length <= PushReachEnum.OK.ordinal()) {
                // return null;
                pushReachETLMap.put(
                        "colcum lenght less " + (PushReachEnum.OK.ordinal() + 1),
                        originalData);

            }
            else {
                try {
                    String originalDataTranf = originalData.replaceAll(",",
                            "\t");
                    // System.out.println(originalDataTranf);

                    String timestampInfoStr = splitSts[PushReachEnum.TIMESTAMP
                            .ordinal()];
                    java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                            .formatTimestamp(timestampInfoStr);
                    // java.util.Map<ConstantEnum, String> formatTimesMap =
                    // TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
                    // BootStrapEnum.class.getName());
                    // dataId
                    String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                    String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                    // if (dateId.equalsIgnoreCase("")) {
                    // return null;
                    // }
                    // hourid
                    int hourId = Integer.parseInt(hourIdStr);
                    logger.info("dateId:" + dateId);
                    logger.info("hourId:" + hourId);

                    // 获取设备类型
                    String platInfo = splitSts[PushReachEnum.DEV.ordinal()];
                    platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
                    // System.out.println(platInfo);
                    // platid
                    int platId = 0;
                    platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
                    logger.info("platId:" + platId);
                    String versionInfo = splitSts[PushReachEnum.VER.ordinal()];
                    // System.out.println(versionInfo);
                    // versionId
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    logger.info("versionId:" + versionId);
                    // qudaoId
                    int qudaoId = SidFormatMobileUtil.getSidByEnum(splitSts,
                            dmQuDaoRuleDAO, PushReachEnum.class.getName());
                    logger.info("qudaoInfo:" + qudaoId);
                    // ipinfo
                    String ipInfoStr = splitSts[PushReachEnum.IP.ordinal()];
                    long ipLong = 0;
                    ipLong = IPFormatUtil.ip2long(ipInfoStr);
                    java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                            .getDMOjb(ipLong);
                    String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                    String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                    String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                    logger.info("ipinfo:" + ipRuleMap);
                    // mac地址
                    String macInfoStr = splitSts[PushReachEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macInfoStr);
                    // String macCode = MACFormatUtil.macFormat(macInfoStr).get(
                    // ConstantEnum.MAC_LONG);
                    // logger.info("MacCode:" + macCode);
                    String macInfor = MACFormatUtil
                            .macFormatToCorrectStr(macInfoStr);

                    // 提取前两个版本信息
                    String versionSub = FormatMobileUtil
                            .versionFormatMobileUtil(versionInfo);
                    // 提取消息类型(messagetype)
                    String messgeTypeStr = FormatMobileUtil
                            .messageFormatMobileUtil(splitSts,
                                    PushReachEnum.class.getName());
                    // 消息是否展示成功（ok）
                    String okStr = FormatMobileUtil.dealWithOk(splitSts,
                            PushReachEnum.class.getName());
                    // pushReachETLSB.append(dateId + "\t");
                    // pushReachETLSB.append(hourId + "\t");
                    // pushReachETLSB.append(platId + "\t");
                    // pushReachETLSB.append(versionId + "\t");
                    // pushReachETLSB.append(qudaoId + "\t");
                    // pushReachETLSB.append(cityId + "\t");
                    // pushReachETLSB.append(macCode + "\t");
                    // pushReachETLSB.append(provinceId + "\t");
                    // pushReachETLSB.append(ispId+ "\t");
                    // pushReachETLSB.append(originalDataTranf.trim() );

                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    // logger.error("error originalData:" + originalData);
                    // logger.error(e.getMessage(), e.getCause());
                    e.printStackTrace();
                    pushReachETLMap.put(e.getMessage(), originalData);

                }
            }
            return pushReachETLMap;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, String> pushReachETLMap = this
                    .getPushReachFormatMap(line);
            if (null != pushReachETLMap && pushReachETLMap.size() > 0) {
                Set<Map.Entry<String, String>> set = pushReachETLMap.entrySet();
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

    public static class PushReachFormatFilterByErrorReduce extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }

        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(PushReachFormatFilterByErrorMR.class);
        job.setJobName("PushReachFormat");
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
        FileInputFormat.addInputPath(job, new Path("input_pushreach"));
        FileOutputFormat.setOutputPath(job, new Path(
                "output_validate_pushreach"));
        job.setMapperClass(PushReachFormatFilterByErrorMap.class);
        job.setReducerClass(PushReachFormatFilterByErrorReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
