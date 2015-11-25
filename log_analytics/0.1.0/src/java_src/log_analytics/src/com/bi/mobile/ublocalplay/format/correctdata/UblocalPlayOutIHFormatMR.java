package com.bi.mobile.ublocalplay.format.correctdata;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.ublocalplay.format.dataenum.UblocalPlayEnum;


public class UblocalPlayOutIHFormatMR {

    public static class UblocalPlayOutIHFormatMap extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        // private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerInfoRuleDAO = null;
        
        private int curDate = 0;
        
        private static Logger logger = Logger
                .getLogger(UblocalPlayOutIHFormatMap.class.getName());

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            
            /* Description: parse fileInputPath ,get current date
             * 
             * Demo: 
             *    fileinputpath: /dw/logs/mobile/origin/app_ublocalplay/2013/05/12
             *    date: 20130512
             * 
            */
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            String filePathStr = fileInputSplit.getPath().toUri().getPath();
            String[] filePathSts = filePathStr.split("/");  
            
            StringBuilder curDateStr = new StringBuilder();
            
            curDateStr.append(filePathSts[6]);
            curDateStr.append(filePathSts[7]);
            curDateStr.append(filePathSts[8]);
            
            this.curDate = Integer.valueOf(curDateStr.toString());            
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

           // this.dmServerInfoRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();

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
                System.out.println("UblocalPlayETLMap rummode is cluster!");
                this.dmPlatyRuleDAO.parseDMObj(new File(
                        ConstantEnum.DM_MOBILE_PLATY.name().toLowerCase()));
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

        public String getUblocalPlayOutIHFormatStr(String originalData) {
            // UblocalPlayETL UblocalPlayETL = null;
            StringBuilder ublocalplayETLStr = new StringBuilder();
            String[] splitSts = originalData.split(",");
            if (splitSts.length < UblocalPlayEnum.SID.ordinal() ) {
                return null;
            }
            try {
                // System.out.println(originalData);
                String originalDataTranf = originalData.replaceAll(",", "\t");
                // System.out.println(originalDataTranf);

                String tmpstampInfoStr = splitSts[UblocalPlayEnum.TIMESTAMP
                        .ordinal()].substring(0, 10);
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(tmpstampInfoStr);
                // java.util.Map<ConstantEnum, String> formatTimesMap =
                // TimestampFormatMobileUtil.getFormatTimesMap(originalData,splitSts,
                // BootStrapEnum.class.getName());
                // dateId
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
                
                int dateid = Integer.valueOf(dateId);
                
                if ((dateid > this.curDate) || (dateid <= (this.curDate-7))) {
                	return null;
                }
                
                // hourid
                int hourId = Integer.parseInt(hourIdStr);
                logger.info("dateId:" + dateId);
                logger.info("hourId:" + hourId);

                String platInfo = splitSts[UblocalPlayEnum.DEV.ordinal()];
                PlatTypeFormatUtil.filterFlash(platInfo);
                platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
                // System.out.println(platInfo);

                // platid
                int platId = 0;
                platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
                logger.info("platId:" + platId);
                String versionInfo = splitSts[UblocalPlayEnum.VER.ordinal()];
//            	if(versionInfo.toLowerCase().contains("_vlc")){
//					throw new Exception("_vlc");
//				}
                // System.out.println(versionInfo);
                // versionId
                long versionId = -0l;
                versionId = IPFormatUtil.ip2long(versionInfo);
                logger.info("versionId:" + versionId);
                // qudaoId
                // String qudaoInfo = splitSts[UblocalPlayEnum.SID.ordinal()];
                // int qudaoId = Integer.parseInt(qudaoInfo);
                // qudaoId = this.dmQuDaoRuleDAO.getDMOjb(qudaoId);
//                int qudaoId;
//                try {
//                    String qudaoInfo = splitSts[UblocalPlayEnum.SID.ordinal()];
//                    qudaoId = Integer.parseInt(qudaoInfo);
//                    qudaoId = this.dmQuDaoRuleDAO.getDMOjb(qudaoId);
//                }
//                catch(Exception e) {
//                    // TODO: handle exception
//                    qudaoId = 1;
//                }
                int qudaoId = SidFormatMobileUtil.getSidByEnum(splitSts, dmQuDaoRuleDAO, UblocalPlayEnum.class.getName());
                logger.info("qudaoInfo:" + qudaoId);
                // ipinfo
                String ipInfoStr = splitSts[UblocalPlayEnum.IP.ordinal()];
                long ipLong = 0;
                ipLong = IPFormatUtil.ip2long(ipInfoStr);

                java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
                logger.info("ipinfo:" + ipLong + "::" + ipRuleMap);
                // mac地址
                String macInfoStr = splitSts[UblocalPlayEnum.MAC.ordinal()];
                MACFormatUtil.isCorrectMac(macInfoStr);
//                String macCode = MACFormatUtil.macFormat(macInfoStr).get(
//                        ConstantEnum.MAC_LONG);
//                logger.info("MacCode:" + macCode);
                String macInfor = MACFormatUtil.macFormatToCorrectStr(macInfoStr);

                // // inforhash
                // String inforHashStr = splitSts[UblocalPlayEnum.IH.ordinal()];
                // java.util.Map<ConstantEnum, String> inforHashMap =
                // this.dmInforHashRuleDAO
                // .getDMOjb(inforHashStr);
                // logger.info("inforhash:" + inforHashMap);
                // // channelId
                // int channelId = Integer.parseInt(inforHashMap
                // .get(ConstantEnum.CHANNEL_ID));
                // logger.info("channelId:" + channelId);
                // // serialID
                // String serialId = inforHashMap.get(ConstantEnum.SERIAL_ID);
                // logger.info("serialId:" + serialId);
                // // meidaID
                // String mediaId = inforHashMap.get(ConstantEnum.MEIDA_ID);
                // logger.info("meidaId:" + mediaId);


                String ublocalplayOKStr = splitSts[UblocalPlayEnum.OK.ordinal()];
                if (ublocalplayOKStr.equalsIgnoreCase("0")) {
                    return null;
                }
                ublocalplayETLStr.append(dateId + "\t");
                ublocalplayETLStr.append(hourId + "\t");
                ublocalplayETLStr.append(platId + "\t");
                ublocalplayETLStr.append(versionId + "\t");
                ublocalplayETLStr.append(qudaoId + "\t");
                // fbufferETLStr.append(channelId + "\t");
                ublocalplayETLStr.append(cityId + "\t");
                ublocalplayETLStr.append(macInfor + "\t");
                // fbufferETLStr.append(mediaId + "\t");
                // fbufferETLStr.append(serialId + "\t");
                ublocalplayETLStr.append(provinceId + "\t");
                ublocalplayETLStr.append(ispId + "\t");
                ublocalplayETLStr.append(originalDataTranf.trim());

            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                logger.error("error originalData:" + originalData);
                logger.error(e.getMessage(), e.getCause());
            }

            return ublocalplayETLStr.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String ublocalplayETLStr = this.getUblocalPlayOutIHFormatStr(line);
            if (null != ublocalplayETLStr && !("".equalsIgnoreCase(ublocalplayETLStr))) {
                context.write(
                        new Text(
                                ublocalplayETLStr.split("\t")[UblocalPlayEnum.TIMESTAMP
                                        .ordinal()]), new Text(ublocalplayETLStr));
            }
        }
    }

    public static class UblocalPlayOutIHFormatReduce extends
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
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stubnull
        Job job = new Job();
        job.setJarByClass(UblocalPlayOutIHFormatMR.class);
        job.setJobName("UblocalPlayETL");
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
        FileInputFormat.addInputPath(job, new Path("2013/05/12"));
      
        FileOutputFormat
                .setOutputPath(job, new Path("output_ublocalplay_new"));
        job.setMapperClass(UblocalPlayOutIHFormatMap.class);
        job.setReducerClass(UblocalPlayOutIHFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
