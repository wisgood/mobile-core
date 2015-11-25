package com.bi.format;

/**   
 * 
 * www.funshion.com
 * All rights reserved
 * @Title: DownloadFormat.java 
 * @Package com.bi.format 
 * @Description: 对download日志进行格式化
 * @author wangxw
 * @date 2013-11-26 上午11:30:35 
 * @input:输入日志路径 /dw/logs/mobile/origin/app_download,/dw/logs/common/dm_common_infohash_new
 * @output:输出日志路径 /dw/logs/format/app_download
 * @executeCmd:hadoop jar downloadformat.jar   -files /disk6/datacenter/mobile/conf/ip_table,/disk6/datacenter/mobile/conf/dm_mobile_plat,/disk6/datacenter/mobile/conf/dm_mobile_qudao    
 * @executeCmd:                     -D mapred.reduce.tasks=10  input_path output_path 
 * @inputFormat:    TIMESTAMP, IP, DEV, MAC, VER, NT, MID, IH, SID, RT, IPHONEIP, CL,FUDID,MESSAGEID
 * @ouputFormat:     DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, ISP_ID, PLAT_ID, QUDAO_ID, VERSION_ID, CLIENT_IP,MAC,FUDID
 * @ouputFormat:     INFOHASH_ID, SERIAL_ID, MEDIA_ID, CHANNEL_ID, MEDIA_NAME,  TIMESTAMP, NET_TYPE, CL, MESSAGE_ID
 */

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.DMInfoHashEnum;
import com.bi.common.logenum.DownloadEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.TimestampFormatUtil;

public class DownloadFormatMR extends Configured implements Tool {

    private static final String SEPERATOR = UtilComstrantsEnum.tabSeparator
            .getValueStr();

    public static class DownloadFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private Text outputKey = null;

        private Text outputValue = null;

        private String filePath;

        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmPlatyRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_PLAT_PATH));
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmQuDaoRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_QUDAO
                    .name().toLowerCase()));
            outputKey = new Text();
            outputValue = new Text();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }

        public String formatLog(String[] originFields) throws Exception {
            try {

                String[] fields = getDefaultFields();
                int length = originFields.length <= DownloadEnum.values().length ? originFields.length
                        : DownloadEnum.values().length;
                System.arraycopy(originFields, 0, fields, 0, length);
                // dateid,hourid
                String timeStamp = fields[DownloadEnum.TIMESTAMP.ordinal()];
                Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                        .formatTimestamp(timeStamp);
                String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
                String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

                // ip_format
                String ipOrigin = fields[DownloadEnum.IP.ordinal()];
                long ipLong = IPFormatUtil.ip2long(ipOrigin);
                Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // client_ip
                String clientIP = null;
                try {
                    clientIP = IPFormatUtil
                            .ipFormat(fields[DownloadEnum.IPHONEIP.ordinal()]);
                }
                catch(Exception e) {
                    // TODO: handle exception
                    clientIP = DefaultFieldValueEnum.IPDefault.getValueStr();

                }
                // platid
                String platOrigin = fields[DownloadEnum.DEV.ordinal()];
                platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
                int platId = 0;
                platId = dmPlatyRuleDAO.getDMOjb(platOrigin);

                // versionId
                String versionOrigin = fields[DownloadEnum.VER.ordinal()];
                long versionId = 0L;
                versionId = IPFormatUtil.ip2long(versionOrigin);
                // infohash
                String infohash = getStringValueOfField(
                        fields[DownloadEnum.IH.ordinal()],
                        DefaultFieldValueEnum.infohashidDefault.getValueStr())
                        .toUpperCase();
                // mac
                String mac = MACFormatUtil
                        .getCorrectMac(fields[DownloadEnum.MAC.ordinal()]);
                // netTypeId
                int netTypeId = (int) getLongValueOfField(
                        fields[DownloadEnum.NT.ordinal()],
                        DefaultFieldValueEnum.netTypeDefault.getValueInt());
                if (netTypeId > 3 || netTypeId < -1)
                    netTypeId = DefaultFieldValueEnum.netTypeDefault
                            .getValueInt();
                // clId
                int clId = (int) getLongValueOfField(
                        fields[DownloadEnum.CL.ordinal()],
                        DefaultFieldValueEnum.CLDefault.getValueInt());
                if (clId > 4 || netTypeId < 1)
                    clId = DefaultFieldValueEnum.CLDefault.getValueInt();
                // messageId
                int messageId = (int) getLongValueOfField(
                        fields[DownloadEnum.MESSAGEID.ordinal()],
                        DefaultFieldValueEnum.messageIdDefault.getValueInt());
                // qudaoId
                int qudaoId = (int) getLongValueOfField(
                        fields[DownloadEnum.SID.ordinal()],
                        DefaultFieldValueEnum.qudaoIdDefault.getValueInt());
                qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
                String fudid = getStringValueOfField(
                        fields[DownloadEnum.FUDID.ordinal()],
                        DefaultFieldValueEnum.fudidDefault.getValueStr());
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
                formatLog.append(fudid);
                formatLog.append(SEPERATOR);
                formatLog.append(infohash);
                formatLog.append(SEPERATOR);
                formatLog.append(timeStamp);
                formatLog.append(SEPERATOR);
                formatLog.append(netTypeId);
                formatLog.append(SEPERATOR);
                formatLog.append(clId);
                formatLog.append(SEPERATOR);
                formatLog.append(messageId);
                return formatLog.toString();
            }
            catch(Exception e) {
                throw e;
            }

        }

        private long getLongValueOfField(String origin, long defaultValue) {
            try {
                return Long.parseLong(origin);
            }
            catch(Exception e) {
                return defaultValue;
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
            String[] fields = new String[DownloadEnum.values().length];
            for (int i = 0; i < DownloadEnum.values().length; i++) {
                fields[i] = "";
            }
            fields[0] = "0";// timestamp
            fields[1] = DefaultFieldValueEnum.IPDefault.getValueStr();// ip
            fields[2] = "other";// plat
            fields[3] = DefaultFieldValueEnum.macCodeDefault.getValueStr();// mac
            fields[4] = DefaultFieldValueEnum.IPDefault.getValueStr();// version
            fields[5] = DefaultFieldValueEnum.netTypeDefault.getValueStr();// nt
            fields[6] = DefaultFieldValueEnum.mediaIdDefault.getValueStr();// mid
            fields[7] = DefaultFieldValueEnum.infohashidDefault.getValueStr();// ih
            fields[8] = DefaultFieldValueEnum.qudaoIdDefault.getValueStr();// sid
            fields[9] = "0";// rt
            fields[10] = DefaultFieldValueEnum.IPDefault.getValueStr();// iphoneip
            fields[11] = DefaultFieldValueEnum.CLDefault.getValueStr();// cl
            fields[12] = DefaultFieldValueEnum.fudidDefault.getValueStr();// fudid
            fields[13] = DefaultFieldValueEnum.messageIdDefault.getValueStr();// messageid
            return fields;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = null;
                if (filePath.toLowerCase().contains("download")) {
                    fields = value.toString().split(
                            UtilComstrantsEnum.comma.getValueStr(), -1);
                    String inforhash;
                    if (fields.length < DownloadEnum.IH.ordinal() + 1)
                        inforhash = DefaultFieldValueEnum.infohashidDefault
                                .getValueStr();
                    else {
                        inforhash = getStringValueOfField(
                                fields[DownloadEnum.IH.ordinal()].toUpperCase(),
                                DefaultFieldValueEnum.infohashidDefault
                                        .getValueStr());
                    }
                    outputKey.set(inforhash);
                    outputValue.set(formatLog(fields));
                }

                else {
                    fields = value.toString().split(SEPERATOR, -1);
                    String inforhash = fields[DMInfoHashEnum.IH.ordinal()]
                            .toUpperCase();
                    outputKey.set(inforhash);
                    outputValue.set(value.toString());

                }
                context.write(outputKey, outputValue);
            }
            catch(Exception e) {
                multipleOutputs.write(new Text(null == e.getMessage() ? "error"
                        : e.getMessage()), new Text(value.toString()),
                        "_error/part");
                e.printStackTrace();
                return;

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            multipleOutputs.close();
        }

    }

    public static class DownloadFormatReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        private Text outputKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String defaultInfohash = DMInfoHashEnum.IH
                    .getInForHashDefaultValue();
            List<String> outputValues = new LinkedList<String>();
            for (Text value : values) {
                String[] fields = value.toString().split(SEPERATOR, -1);
                if (fields.length == 5) {
                    defaultInfohash = fields[0] + SEPERATOR + fields[1]
                            + SEPERATOR + fields[2] + SEPERATOR + fields[3]
                            + SEPERATOR + fields[4];
                }
                else {
                    outputValues.add(value.toString());
                }

            }

            Iterator<String> iterator = outputValues.iterator();
            while (iterator.hasNext()) {
                String[] fields = iterator.next().split(SEPERATOR);
                fields[11] = defaultInfohash;
                String value = StringUtils.join(fields, SEPERATOR);
                outputKey.set(value);
                context.write(outputKey, NullWritable.get());
            }

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {

        int nRet = ToolRunner.run(new Configuration(), new DownloadFormatMR(),
                args);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "log-analytics-format-download");
        job.setJarByClass(DownloadFormatMR.class);
        HdfsUtil.deleteDir(args[1]);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DownloadFormatMapper.class);
        job.setReducerClass(DownloadFormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        int result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }
}