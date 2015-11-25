/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DbufferQualityFormat.java 
 * @Package com.bi.mobilequality.dbuffer.format.correctdata 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-10 下午2:59:19 
 */
package com.bi.mobilequality.dbuffer.format.correctdata;

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
import org.hsqldb.lib.StringUtil;

import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.MACFormatUtil;
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.mobilequality.dbuffer.format.dataenum.DbufferEnum;

/**
 * @ClassName: DbufferQualityFormat
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-10 下午2:59:19
 */
public class DbufferQualityFormatMR {

    public static class DbufferQualityFormatMap extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerInfoRuleDAO = null;

        private static Logger logger = Logger
                .getLogger(DbufferQualityFormatMap.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();

            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            this.dmServerInfoRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();

            if (this.isLocalRunMode(context)) {
                String dmMobilePlayFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
                this.dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));

                String dmIpTableFilePath = context.getConfiguration().get(
                        ConstantEnum.IPTABLE_FILEPATH.name());
                this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));

                String dmServerFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name());
                this.dmServerInfoRuleDAO.parseDMObj(new File(dmServerFilePath));
            }
            else {
                logger.info("PlayTMOtherETLMap rummode is cluster!");
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);

                this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
                        .name().toLowerCase()));
                this.dmServerInfoRuleDAO.parseDMObj(new File(
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

        public String getDbufferQualityFormatStr(String originalData) {
            // PlayTMOtherETL playTMETL = null;
            StringBuilder playTMETLSB = new StringBuilder();
            String[] splitSts = originalData.split(",");
            if (splitSts.length <= DbufferEnum.BTM.ordinal()) {
                return null;
            }
            try {
                // //System.out.println(originalData);
                splitSts = SpecialVersionRecomposeFormatMobileUtil
                        .recomposeBySpecialVersionIndex(splitSts,
                                DbufferEnum.class.getName());

                String originalDataTranf = org.apache.commons.lang.StringUtils
                        .join(splitSts, "\t");
               
                // System.out.println(originalDataTranf);

                String tmpstampInfoStr = splitSts[DbufferEnum.TIMESTAMP
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(tmpstampInfoStr);
                // java.util.Map<ConstantEnum, String> formatTimesMap =
                // TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
                // BootStrapEnum.class.getName());
                // dataId
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

                // hourid
                int hourId = Integer.parseInt(hourIdStr);
                logger.info("dateId:" + dateId);
                logger.info("hourId:" + hourId);

                String platInfo = splitSts[DbufferEnum.DEV.ordinal()];
                PlatTypeFormatUtil.filterFlash(platInfo);
                PlatTypeFormatUtil.filterOtherInfo(platInfo, "winphone");
                platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

                // platid
                int platId = 0;
                platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
                logger.info("platId:" + platId);
                String versionInfo = splitSts[DbufferEnum.VER.ordinal()];

                long versionId = 0l;
                versionId = IPFormatUtil.ip2long(versionInfo);
                logger.info("versionId:" + versionId);

                // ipinfo
                String ipInfoStr = splitSts[DbufferEnum.IP.ordinal()];
                long ipLong = 0;
                ipLong = IPFormatUtil.ip2long(ipInfoStr);

                java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                // String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
                // mac地址
                String macInfoStr = splitSts[DbufferEnum.MAC.ordinal()];
                MACFormatUtil.isCorrectMac(macInfoStr);
                String macInfor = MACFormatUtil
                        .macFormatToCorrectStr(macInfoStr);
                long btmlong = FormatMobileUtil
                        .parseDoubleToLong(splitSts,
                                DbufferEnum.class.getName(),
                                DbufferEnum.BTM.toString());

                long bposlong = FormatMobileUtil.parseDoubleToLong(splitSts,
                        DbufferEnum.class.getName(),
                        DbufferEnum.BPOS.toString());

                String ipFormatStr = IPFormatUtil.ipFormat(ipInfoStr);

                String serverIpStr = splitSts[DbufferEnum.SERVERIP.ordinal()];
                long serverId = 0;
                serverId = IPFormatUtil.ip2long(serverIpStr);
                Map<ConstantEnum, String> serverInfoMap = this.dmServerInfoRuleDAO
                        .getDMOjb(serverId + "");
                serverId = Long.parseLong(serverInfoMap
                        .get(ConstantEnum.SERVER_ID));
                logger.info("serverId:" + serverId);
                FormatMobileUtil.filerNoNumber(splitSts[DbufferEnum.NT.ordinal()], "NetWork type ");
                playTMETLSB.append(dateId + "\t");
                playTMETLSB.append(hourId + "\t");
                playTMETLSB.append(platId + "\t");
                playTMETLSB.append(versionId + "\t");
                playTMETLSB.append(provinceId + "\t");
                // playTMETLSB.append(cityId + "\t");
                playTMETLSB.append(ispId + "\t");
                playTMETLSB.append(serverId + "\t");
                playTMETLSB.append(btmlong + "\t");
                playTMETLSB.append(bposlong + "\t");
                playTMETLSB.append(macInfor + "\t");
                playTMETLSB.append(ipFormatStr + "\t");
                playTMETLSB.append(originalDataTranf.trim());

            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                logger.error("error originalData:" + originalData);
                logger.error(e.getMessage(), e.getCause());
            }

            return playTMETLSB.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String playTMETLStr = this.getDbufferQualityFormatStr(line);
            if (null != playTMETLStr && !("".equalsIgnoreCase(playTMETLStr))) {
                context.write(
                        new Text(playTMETLStr.split("\t")[DbufferEnum.TIMESTAMP
                                .ordinal()]), new Text(playTMETLStr));
            }
        }
    }

    public static class DbufferQualityFormatReduce extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text());
            }

        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Job job = new Job();
        job.setJarByClass(DbufferQualityFormatMR.class);
        job.setJobName("DbufferQualityFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        // 设置配置文件默认路径
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
                "conf/ip_table");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name(),
                "conf/dm_mobile_server");
        FileInputFormat.addInputPath(job, new Path("input_dbuffer"));
        FileOutputFormat.setOutputPath(job, new Path("output_dbuffer_quality"));
        job.setMapperClass(DbufferQualityFormatMap.class);
        job.setReducerClass(DbufferQualityFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
