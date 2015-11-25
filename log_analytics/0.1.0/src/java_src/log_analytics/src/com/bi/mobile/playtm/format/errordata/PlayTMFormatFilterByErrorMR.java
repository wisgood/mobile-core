package com.bi.mobile.playtm.format.errordata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.playtm.format.dataenum.PlayTMEnum;


public class PlayTMFormatFilterByErrorMR {

    public static class PlayTMFormatFilterByErrorMap extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmInforHashRuleDAO = null;

        private static Logger logger = Logger
                .getLogger(PlayTMFormatFilterByErrorMap.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            // this.dmInforHashRuleDAO = new DMInforHashRuleDAOImpl<String,
            // Map<ConstantEnum, String>>();

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
                // String dmInforHashFilePath = context.getConfiguration().get(
                // ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name());
                // this.dmInforHashRuleDAO
                // .parseDMObj(new File(dmInforHashFilePath));
            }
            else {
                logger.info("PlayTMOtherETLMap rummode is cluster!");
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                // System.out.println(ConstantEnum.DM_MOBILE_PLATY
                // .name().toLowerCase());
                // System.out.println(dmMobilePlayFile==null?"kong":"ddd");
                this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
                this.dmQuDaoRuleDAO.parseDMObj(new File(
                        ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
                this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE
                        .name().toLowerCase()));
                // this.dmInforHashRuleDAO.parseDMObj(new File(
                // ConstantEnum.DM_COMMON_INFOHASH.name().toLowerCase()));
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

        public Map<String, String> getPlayTMOtherETLStrErrorString(
                String originalData) {
            // PlayTMOtherETL playTMETL = null;
            Map<String, String> exitETLFilterMap = new HashMap<String, String>();
            String[] splitSts = originalData.split(",");
            int column = 0;
            // StringBuilder playTMETLSB = new StringBuilder();
            // String[] splitSts = originalData.split(",");
            if (splitSts.length <= PlayTMEnum.VTM.ordinal()) {
                exitETLFilterMap.put(
                        "column length is" + splitSts.length + "<"
                                + (PlayTMEnum.VTM.ordinal() + 1) + ":"
                                + originalData.toString(), "length is short");

                // exitETLFilterMap.put("column length is" + splitSts.length +
                // "<"
                // + (PlayTMEnum.SID.ordinal() + 1), originalData);
                // exitETLFilterMap.put(originalData, "column length is"
                // + splitSts.length + "<"
                // + (PlayTMEnum.SID.ordinal() + 1));
            }
            else {
                try {

                    // //System.out.println(originalData);
                    String originalDataTranf = originalData.replaceAll(",",
                            "\t");
                    // System.out.println(originalDataTranf);

                    String tmpstampInfoStr = splitSts[PlayTMEnum.TIMESTAMP
                            .ordinal()];
                    java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                            .formatTimestamp(tmpstampInfoStr);
                    column = PlayTMEnum.TIMESTAMP.ordinal();

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

                    String platInfo = splitSts[PlayTMEnum.DEV.ordinal()];
                    column = PlayTMEnum.DEV.ordinal();
                    PlatTypeFormatUtil.filterFlash(platInfo);

                    platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
                    // System.out.println(platInfo);

                    // platid
                    int platId = 0;
                    platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
                    logger.info("platId:" + platId);
                    String versionInfo = splitSts[PlayTMEnum.VER.ordinal()];
                    // if(versionInfo.toLowerCase().contains("_vlc")){
                    // throw new Exception("_vlc");
                    // }
                    // System.out.println(versionInfo);
                    // versionId
                    long versionId = -0l;
                    versionId = IPFormatUtil.ip2long(versionInfo);
                    logger.info("versionId:" + versionId);
                    // qudaoId

                    int qudaoId = SidFormatMobileUtil.getSidByEnum(splitSts,
                            dmQuDaoRuleDAO, PlayTMEnum.class.getName());
                    logger.info("qudaoInfo:" + qudaoId);
                    // ipinfo
                    String ipInfoStr = splitSts[PlayTMEnum.IP.ordinal()];
                    long ipLong = 0;
                    ipLong = IPFormatUtil.ip2long(ipInfoStr);

                    java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                            .getDMOjb(ipLong);
                    String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                    String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                    String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                    logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
                    // mac地址
                    String macInfoStr = splitSts[PlayTMEnum.MAC.ordinal()];
                    MACFormatUtil.isCorrectMac(macInfoStr);
                    // String macCode = MACFormatUtil.macFormat(macInfoStr).get(
                    // ConstantEnum.MAC_LONG);
                    // logger.info("MacCode:" + macCode);
                    String macInfor = MACFormatUtil
                            .macFormatToCorrectStr(macInfoStr);

                    // inforhash
                    // String inforHashStr = splitSts[PlayTMEnum.IH.ordinal()];
                    // java.util.Map<ConstantEnum, String> inforHashMap =
                    // this.dmInforHashRuleDAO
                    // .getDMOjb(inforHashStr);
                    // logger.info("inforhash:" + inforHashMap);
                    // // channelId
                    // int channelId = Integer.parseInt(inforHashMap
                    // .get(ConstantEnum.CHANNEL_ID));
                    // logger.info("channelId:" + channelId);
                    // // serialID
                    // String serialId =
                    // inforHashMap.get(ConstantEnum.SERIAL_ID);
                    // logger.info("serialId:" + serialId);
                    // // meidaID
                    // String mediaId = inforHashMap.get(ConstantEnum.MEIDA_ID);
                    // logger.info("meidaId:" + mediaId);
                    String vtmStr = splitSts[PlayTMEnum.VTM.ordinal()];
                    double vtmDouble = Double.parseDouble(vtmStr);
                    long vtmlong = (long) vtmDouble;
                    if (!(vtmlong >= 0 && vtmlong < 2147483647)) {

                        // throw new Exception("播放时长不满足指定范围!");
                        exitETLFilterMap.put("play time is error:"
                                + originalData.toString(), "播放时长不满足指定范围");

                    }

                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    exitETLFilterMap.put(e.getMessage() + ":" + originalData,
                            "exception");
                    // exitETLFilterMap.put(e.getMessage(), originalData);
                }
            }

            return exitETLFilterMap;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, String> exitETLMap = this
                    .getPlayTMOtherETLStrErrorString(line);
            if (null != exitETLMap && exitETLMap.size() > 0) {
                Set<Map.Entry<String, String>> set = exitETLMap.entrySet();
                for (Iterator<Map.Entry<String, String>> it = set.iterator(); it
                        .hasNext();) {
                    Map.Entry<String, String> entry = (Map.Entry<String, String>) it
                            .next();
                    // context.write(new Text(entry.getValue()),
                    // new Text(entry.getKey()));
                    context.write(new Text(entry.getKey()),
                            new Text(entry.getValue()));
                }
            }
        }
    }

    public static class PlayTMFormatFilterByErrorReduce extends
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
        job.setJarByClass(PlayTMFormatFilterByErrorMR.class);
        job.setJobName("EroroPlayTMOtherETL");
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
        // job.setPartitionerClass( CustomPartioner.class );
        FileInputFormat.addInputPath(job, new Path("input_playtm"));
        FileOutputFormat.setOutputPath(job, new Path("output_playtm_18q"));
        job.setMapperClass(PlayTMFormatFilterByErrorMap.class);
        job.setReducerClass(PlayTMFormatFilterByErrorReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}