/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayTMFormatMR.java 
 * @Package com.bi.mobilequality.playtm.format 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-10 上午11:25:05 
 */
package com.bi.mobilequality.playtm.format.errordata;

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
import com.bi.mobile.comm.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.mobile.playtm.format.dataenum.PlayTMEnum;

import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.mobile.fbuffer.format.dataenum.FbufferEnum;

/**
 * @ClassName: PlayTMFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-10 上午11:25:05
 */
public class PlayTMQualityFilterByErrorFormatMR {

    public static class PlayTMQualityFilterByErrorFormatMap extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private static Logger logger = Logger
                .getLogger(PlayTMQualityFilterByErrorFormatMap.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();

            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            if (this.isLocalRunMode(context)) {
                String dmMobilePlayFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
                this.dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));

                String dmIpTableFilePath = context.getConfiguration().get(
                        ConstantEnum.IPTABLE_FILEPATH.name());
                this.dmIPRuleDAO.parseDMObj(new File(dmIpTableFilePath));

                String dmServerFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name());

            }
            else {
                logger.info("PlayTMOtherETLMap rummode is cluster!");
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);

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

        public Map<String, String> getPlayTMQualityFilterByErrorFormatMap(
                String originalData) {
            // PlayTMOtherETL playTMETL = null;
            // StringBuilder playTMETLSB = new StringBuilder();
            Map<String, String> playTMEQualityFormatMap = new WeakHashMap<String, String>();
            String[] splitSts = originalData.split(",");
            if (splitSts.length <= PlayTMEnum.TU.ordinal()) {
                playTMEQualityFormatMap.put("colcum lenght less "
                        + (PlayTMEnum.TU.ordinal() + 1), originalData);
            }
            else {
                try {
                    splitSts = SpecialVersionRecomposeFormatMobileUtil
                            .recomposeBySpecialVersionIndex(splitSts,
                                    PlayTMEnum.class.getName());
                    // //System.out.println(originalData);
                    // String originalDataTranf = originalData.replaceAll(",",
                    // "\t");
                    // System.out.println(originalDataTranf);

                    String tmpstampInfoStr = splitSts[PlayTMEnum.TIMESTAMP
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

                    String platInfo = splitSts[PlayTMEnum.DEV.ordinal()];
                    PlatTypeFormatUtil.filterFlash(platInfo);
                    PlatTypeFormatUtil.filterOtherInfo(platInfo, "winphone");
                    platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

                    // platid
                    int platId = 0;
                    platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
                    logger.info("platId:" + platId);
                    String versionInfo = splitSts[PlayTMEnum.VER.ordinal()];

                    long versionId = 0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    logger.info("versionId:" + versionId);

                    // ipinfo
                    String ipInfoStr = splitSts[PlayTMEnum.IP.ordinal()];
                    long ipLong = 0;
                    ipLong = IPFormatUtil.ip2long(ipInfoStr);

                    java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                            .getDMOjb(ipLong);
                    String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                    // String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                    String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                    logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
                    // mac地址
                    String macInfoStr = splitSts[PlayTMEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macInfoStr);
                    String macInfor = MACFormatUtil
                            .macFormatToCorrectStr(macInfoStr);
                    long vtmlong = FormatMobileUtil.parseDoubleToLong(splitSts,
                            PlayTMEnum.class.getName(),
                            PlayTMEnum.VTM.toString());
                    long pnlong = FormatMobileUtil.parseDoubleToLong(splitSts,
                            PlayTMEnum.class.getName(),
                            PlayTMEnum.PN.toString());
                    long tulong = FormatMobileUtil.parseDoubleToLong(splitSts,
                            PlayTMEnum.class.getName(),
                            PlayTMEnum.TU.toString());

                    String ipFormatStr = IPFormatUtil.ipFormat(ipInfoStr);
                    FormatMobileUtil.filerNoNumber(
                            splitSts[PlayTMEnum.NT.ordinal()], "NetWork type ");
                    // playTMETLSB.append(dateId + "\t");
                    // playTMETLSB.append(hourId + "\t");
                    // playTMETLSB.append(platId + "\t");
                    // playTMETLSB.append(versionId + "\t");
                    // playTMETLSB.append(cityId + "\t");
                    // playTMETLSB.append(ispId + "\t");
                    // playTMETLSB.append(vtmlong + "\t");
                    // playTMETLSB.append(pnlong + "\t");
                    // playTMETLSB.append(tulong + "\t");
                    // playTMETLSB.append(macInfor + "\t");
                    // playTMETLSB.append(ipFormatStr + "\t");
                    // playTMETLSB.append(originalDataTranf.trim());

                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    logger.error("error originalData:" + originalData);
                    logger.error(e.getMessage(), e.getCause());
                    if (e.getMessage().contains("multiple points")) {
                        e.printStackTrace();
                    }
                    playTMEQualityFormatMap.put(e.getMessage(), originalData);
                }
            }
            return playTMEQualityFormatMap;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, String> playTMEQualityFormatMap = this
                    .getPlayTMQualityFilterByErrorFormatMap(line);
            if (null != playTMEQualityFormatMap
                    && playTMEQualityFormatMap.size() > 0) {
                Set<Map.Entry<String, String>> set = playTMEQualityFormatMap
                        .entrySet();
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

    public static class PlayTMQualityFilterByErrorFormatReduce extends
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
        // TODO Auto-generated method stub
        Job job = new Job();
        job.setJarByClass(PlayTMQualityFilterByErrorFormatMR.class);
        job.setJobName("PlayTMQualityFilterByErrorFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        // 设置配置文件默认路径
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
                "conf/ip_table");

        FileInputFormat.addInputPath(job, new Path("input_playtm"));
        FileOutputFormat.setOutputPath(job, new Path(
                "output_playtm_quality_error"));
        job.setMapperClass(PlayTMQualityFilterByErrorFormatMap.class);
        job.setReducerClass(PlayTMQualityFilterByErrorFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
