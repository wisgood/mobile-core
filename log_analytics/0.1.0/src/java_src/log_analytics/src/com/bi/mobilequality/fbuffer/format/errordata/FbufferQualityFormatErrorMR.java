package com.bi.mobilequality.fbuffer.format.errordata;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.regex.Pattern;

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
import com.bi.mobilequality.fbuffer.format.dataenum.FbufferEnum;
import com.bi.mobilequality.fbuffer.format.dataenum.FbufferQualityFormatEnum;

public class FbufferQualityFormatErrorMR {

    public static class FbufferQualityFormatErrorMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private static Logger logger = Logger
                .getLogger(FbufferQualityFormatErrorMR.class);

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerdebugRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmServerdebugRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();

            if (isLocalRunMode(context)) {
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
                dmServerdebugRuleDAO.parseDMObj(new File(dmServerFilePath));
            }
            else {
                System.out.println("FbufferETLMap rummode is cluster!");
                dmPlatyRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase()));
                dmQuDaoRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_QUDAO
                        .name().toLowerCase()));
                dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                        .toLowerCase()));
                dmServerdebugRuleDAO.parseDMObj(new File(
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

        private String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";
            try {
                
                fields = SpecialVersionRecomposeFormatMobileUtil
                        .recomposeBySpecialVersionIndex(fields,
                                FbufferEnum.class.getName());
                String timeStampOrigin = fields[FbufferEnum.TIMESTAMP.ordinal()];
                String versionOrigin = fields[FbufferEnum.VER.ordinal()];
                String platOrigin = fields[FbufferEnum.DEV.ordinal()];
                if(platOrigin.contains("flash")||platOrigin.contains("winphone"))
                    throw new Exception("contain flash or winphone");
                String macOrigin = fields[FbufferEnum.MAC.ordinal()];
                String bPosOrigin = fields[FbufferEnum.BPOS.ordinal()];
                String btmOrigin = fields[FbufferEnum.BTM.ordinal()];
                String serverIpOrigin = fields[FbufferEnum.SERVERIP.ordinal()];
                String ipOrigin = fields[FbufferEnum.IP.ordinal()];
                String netTypeOrigin = fields[FbufferEnum.NT.ordinal()];
                String patternStr = "-?\\d+";
                if (!Pattern.matches(patternStr, netTypeOrigin)) {
                    throw new Exception("nettime must be a number");
                }

                // data_id ,hour_id

                Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                        .formatTimestamp(timeStampOrigin);

                String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
                String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

                logger.debug("dateId:" + dateId);
                logger.debug("hourId:" + hourId);

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
                Map<ConstantEnum, String> serverdebugMap = dmServerdebugRuleDAO
                        .getDMOjb(serverId + "");
                serverId = Long.parseLong(serverdebugMap
                        .get(ConstantEnum.SERVER_ID));

                // btm_format,bpos_format
                int btmFormat = processBtm(btmOrigin);
                if (btmFormat<0) {
                    throw new Exception("btime cannot be a negative");
                }

                int bposFormat = processBtm(bPosOrigin);
                

                return empty;
            }
            catch(Exception e) {
                // TODO: handle exception
                return e.getMessage() + "\t" + originLog;
            }

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {
            String originLog = value.toString().replaceAll(",", "\t");
            if (lengthMeet(originLog.split("\t").length)) {
            }
            else {
                context.write(new Text("column lengh is short"), new Text(
                        originLog));
                return ;
            }
            String formatLog = logFormat(originLog);
            if (null != formatLog && !("".equalsIgnoreCase(formatLog))) {
                context.write(new Text(formatLog.split("\t")[FbufferQualityFormatEnum.DATA_ID
                                                             .ordinal()]), new Text(
                        formatLog));
            }

        }

        private boolean lengthMeet(int length) {

            return length >= FbufferEnum.BTM.ordinal() + 1;
        }

        private int processBtm(String origin) {
            double result = Double.valueOf(origin);
            return (int) result;
        }
    }

    public static class FbufferQualityFormatErrorReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }

        }

    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(FbufferQualityFormatErrorMR.class);
        job.setJobName("FbufferQualityFormat-MobileQuality");
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
                "conf/dm_common_debughash");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name(),
                "conf/dm_mobile_server");
        FileInputFormat.addInputPath(job, new Path("input_fbuffer"));
        FileOutputFormat
                .setOutputPath(job, new Path("mq_output_fbuffer_error_2"));
        job.setMapperClass(FbufferQualityFormatErrorMapper.class);
        job.setReducerClass(FbufferQualityFormatErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
