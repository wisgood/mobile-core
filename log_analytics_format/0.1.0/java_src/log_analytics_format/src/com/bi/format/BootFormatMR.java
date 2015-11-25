package com.bi.format;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.aspectj.apache.bcel.generic.ReturnaddressType;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.logenum.BootEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.StringUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * 
 * www.funshion.com All rights reserved
 * 
 * @Title: BootFromatMR.java
 * @Package com.bi.format
 * @Description: 对客户端boot日志进行格式化
 * @author wangxw
 * @date 2013-11-26 上午11:30:35
 * @input:输入日志路径 
 *               /dw/logs/mobile/origin/app_download,/dw/logs/common/dm_common_infohash_new
 * @output:输出日志路径 /dw/logs/format/app_download
 * @executeCmd:hadoop jar BootFormat.jar -files
 *                    /disk6/datacenter/mobile/conf/ip_table
 *                    ,/disk6/datacenter/mobile
 *                    /conf/dm_mobile_plat,/disk6/datacenter
 *                    /mobile/conf/dm_mobile_qudao
 * @executeCmd: -D mapred.reduce.tasks=10 input_path output_path
 * @inputFormat: TIMESTAMP, IP, DEV, MAC, VER, NT, MID, IH, SID, RT, IPHONEIP,
 *               CL,FUDID,MESSAGEID
 * @ouputFormat: DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, ISP_ID, PLAT_ID,
 *               QUDAO_ID, VERSION_ID, CLIENT_IP,MAC,FUDID
 * @ouputFormat: INFOHASH_ID, SERIAL_ID, MEDIA_ID, CHANNEL_ID, MEDIA_NAME,
 *               TIMESTAMP, NET_TYPE, CL, MESSAGE_ID
 */

public class BootFormatMR extends Configured implements Tool {

    private static final String SEPERATOR = UtilComstrantsEnum.tabSeparator.getValueStr();

    public static class BootFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private Text outputKey = null;

        private Text outputValue = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmPlatyRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_PLAT_PATH));
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name().toLowerCase()));
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmQuDaoRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
            outputKey = new Text();
            outputValue = new Text();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
        }

        public String formatLog(String originLog) throws Exception {
            String[] originFields = StringUtil.splitLog(originLog, ',');
            String[] fields = getDefaultFields();
            int length = originFields.length <= BootEnum.values().length ? originFields.length : BootEnum.values().length;
            System.arraycopy(originFields, 0, fields, 0, length);
            // dateid,hourid
            String timeStamp = fields[BootEnum.TIMESTAMP.ordinal()];
            Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil.formatTimestamp(timeStamp);
            String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
            String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

            // ip_format
            String ipOrigin = fields[BootEnum.IP.ordinal()];
            long ipLong = IPFormatUtil.ip2long(ipOrigin);
            Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

            // client_ip
            String clientIP = null;
            try {
                clientIP = ipOrigin;
            }
            catch(Exception e) {
                // TODO: handle exception
                clientIP = DefaultFieldValueEnum.IPDefault.getValueStr();

            }
            // platid
            int platId = 0;

            // versionId
            String versionOrigin = fields[BootEnum.VERSION.ordinal()];
            long versionId = 0L;
            versionId = IPFormatUtil.ip2long(versionOrigin);
            // mac
            String mac = MACFormatUtil.getCorrectMac(fields[BootEnum.MAC.ordinal()]);
            // qudaoId
            int qudaoId = (int) getLongValueOfField(fields[BootEnum.CHANNEL.ordinal()], DefaultFieldValueEnum.qudaoIdDefault.getValueInt());
            qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
            StringBuilder formatLog = new StringBuilder();
            formatLog.append(dateId);
            formatLog.append(SEPERATOR);
            formatLog.append(hourId);
            formatLog.append(SEPERATOR);
            formatLog.append(provinceId);
            formatLog.append(SEPERATOR);
            formatLog.append(cityId);
            formatLog.append(SEPERATOR);
            formatLog.append(ispId);
            formatLog.append(SEPERATOR);
            formatLog.append(platId);
            formatLog.append(SEPERATOR);
            formatLog.append(qudaoId);
            formatLog.append(SEPERATOR);
            formatLog.append(versionId);
            formatLog.append(SEPERATOR);
            formatLog.append(clientIP);
            formatLog.append(SEPERATOR);
            formatLog.append(mac);
            formatLog.append(SEPERATOR);
            formatLog.append(processStartType(fields[BootEnum.STARTTYPE.ordinal()]));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.OS.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.ERROR_CODE.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.HC.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.HARDWARE_ACCELERATION.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.YY.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(getLongValueOfField(fields[BootEnum.TRAY_LIMIT.ordinal()], -999));
            formatLog.append(SEPERATOR);
            formatLog.append(timeStamp);
            return formatLog.toString();

        }

        private long getLongValueOfField(String origin, long defaultValue) {
            try {
                return Long.parseLong(origin);
            }
            catch(Exception e) {
                return defaultValue;
            }

        }

        private int processStartType(String startType) {
            if (null == startType)
                return -999;
            if (startType.equals("0") || startType.equals("2") || startType.equals("3") || startType.equals("1"))
                return Integer.parseInt(startType);
            else if (startType.toLowerCase().contains("startbywindows"))
                return 4;
            else if (startType.toLowerCase().contains("startbywindowstray"))
                return 5;
            else if (startType.toLowerCase().equals(""))
                return 6;
            else if (startType.toLowerCase().contains("startbyinstall"))
                return 7;
            else if (startType.toLowerCase().contains(".rmvb"))
                return 8;
            else if (startType.toLowerCase().contains(".mp4"))
                return 9;
            else if (startType.toLowerCase().contains(".fc!"))
                return 10;
            else if (startType.toLowerCase().contains(".fsp"))
                return 11;
            else if (startType.toLowerCase().contains("fsp:"))
                return 12;
            else if (startType.toLowerCase().contains(".mediadir"))
                return 13;
            else if (startType.toLowerCase().contains("desktop"))
                return 14;
            else if (startType.toLowerCase().contains("progromgroup"))
                return 15;
            else if (startType.toLowerCase().contains("midnight"))
                return 16;
            else if (startType.toLowerCase().contains("green"))
                return 17;
            else if (startType.toLowerCase().contains("funpop"))
                return 18;
            else if (startType.toLowerCase().contains("quicklaunch"))
                return 19;
            else if (startType.toLowerCase().contains("startmenu"))
                return 20;
            else if (startType.toLowerCase().contains("startbyinstalltray"))
                return 21;
            else {
                return -999;
            }

        }

        private String getStringValueOfField(String origin, String defaultValue) {
            if (null == origin) {
                return defaultValue;
            }
            else if ("".equals(origin)) {
                return defaultValue;
            }
            else if ("null".equals(origin)) {
                return defaultValue;
            }
            else {
                return origin;
            }
        }

        private String[] getDefaultFields() {
            String[] fields = new String[BootEnum.values().length];
            for (int i = 0; i < BootEnum.values().length; i++) {
                fields[i] = "";
            }
            fields[0] = "0";// timestamp
            fields[1] = DefaultFieldValueEnum.IPDefault.getValueStr();// ip
            fields[2] = DefaultFieldValueEnum.macCodeDefault.getValueStr();// plat
            fields[3] = DefaultFieldValueEnum.IPDefault.getValueStr();// mac
            fields[4] = DefaultFieldValueEnum.qudaoIdDefault.getValueStr();// version
            fields[5] = "-999";// os
            fields[6] = "-999";// starttype
            fields[7] = "-999";// error_code
            fields[8] = "-999";// hc
            fields[9] = "-999";// hardware_acceration
            fields[10] = "-999";// yy
            fields[11] = "-999";// tray_limit
            return fields;

            // TIMESTAMP, IP, MAC, VERSION, CHANNEL, OS, STARTTYPE, ERROR_CODE,
            // HC, HARDWARE_ACCELERATION, YY, TRAY_LIMIT
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String originLog = value.toString();
            if (!lengthMeet(StringUtil.splitLog(originLog, ',').length)) {
                multipleOutputs.write(new Text("short\t" + originLog), NullWritable.get(), "_error/part");
                return;
            }
            String formatLog;
            try {
                formatLog = formatLog(originLog);
                context.write(new Text(formatLog), NullWritable.get());
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(e.getMessage() + "\t" + originLog), NullWritable.get(), "_error/part");
                return;

            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

        private boolean lengthMeet(int length) {

            return length == 9 || length == 11 || length == 12;
        }

    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {

        int nRet = ToolRunner.run(new Configuration(), new BootFormatMR(), args);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "log-analytics-format-boot");
        job.setJarByClass(BootFormatMR.class);
        HdfsUtil.deleteDir(args[1]);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BootFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        int result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }

}
