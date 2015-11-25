package com.bi.format;

/**   
 * All rights reserved
 * www.funshion.com
 *
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
 * @ouputFormat:     DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, ISP_ID, PLAT_ID, QUDAO_ID, VERSION_ID, INFOHASH_ID, 
 * @ouputFormat:     SERIAL_ID, MEDIA_ID, CHANNEL_ID, MEDIA_NAME, MAC_CODE, FUDID, TIMESTAMP, NET_TYPE, CL, MESSAGE_ID
 */
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import com.bi.common.constant.DimFilePath;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.DMInforHashEnum;
import com.bi.common.logenum.DownloadEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

public class DownloadFormat extends Configured implements Tool {

    private static final String TAB_SEPARATOR = "\t";

    private static final String COMMA_SEPARATOR = ",";

    private static final String DEFAULT_INFOHASH = "00000000000000000000000000000000";

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
                        fields[DownloadEnum.IH.ordinal()], DEFAULT_INFOHASH)
                        .toUpperCase();
                // mac
                String mac = MACFormatUtil
                        .getCorrectMac(fields[DownloadEnum.MAC.ordinal()]);
                // netTypeId
                int netTypeId = (int) getLongValueOfField(
                        fields[DownloadEnum.NT.ordinal()], -1);
                if (netTypeId > 3 || netTypeId < -1)
                    netTypeId = -1;
                // clId
                int clId = (int) getLongValueOfField(
                        fields[DownloadEnum.CL.ordinal()], -1);
                if (clId > 4 || netTypeId < 1)
                    clId = -1;
                // messageId
                int messageId = (int) getLongValueOfField(
                        fields[DownloadEnum.MESSAGEID.ordinal()], -1);
                // qudaoId
                int qudaoId = (int) getLongValueOfField(
                        fields[DownloadEnum.SID.ordinal()], -1);
                qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
                String fudid = getStringValueOfField(
                        fields[DownloadEnum.FUDID.ordinal()], "0");
                StringBuilder formatLog = new StringBuilder();
                formatLog.append(dateId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(hourId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(provinceId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(cityId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(ispId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(platId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(qudaoId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(versionId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(infohash);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(mac);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(fudid);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(timeStamp);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(netTypeId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(clId);
                formatLog.append(TAB_SEPARATOR);
                formatLog.append(messageId);
                return formatLog.toString();
            }
            catch(Exception e) {
                // TODO: handle exception
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
            if (null == origin)
                return defaultValue;
            else if ("".equals(origin))
                return defaultValue;
            else
                return origin;

        }

        private String[] getDefaultFields() {
            String[] fields = new String[DownloadEnum.values().length];
            for (int i = 0; i < DownloadEnum.values().length; i++) {
                fields[i] = "";
            }
            fields[0] = "0";// timestamp
            fields[1] = "0.0.0.0";// ip
            fields[2] = "other";// plat
            fields[3] = "00:00:00:00:00:00";// mac
            fields[4] = "0.0.0.0";// version
            fields[5] = "0";// nt
            fields[6] = "0";// mid
            fields[7] = DEFAULT_INFOHASH;// ih
            fields[8] = "-1";// sid
            fields[9] = "0";// rt
            fields[10] = "0";// iphoneip
            fields[11] = "0";// cl
            fields[12] = "0";// fudid
            fields[13] = "0";// messageid
            return fields;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = null;
                if (filePath.toLowerCase().contains("download")) {
                    fields = value.toString().split(COMMA_SEPARATOR, -1);
                    String inforhash;
                    if (fields.length < DownloadEnum.IH.ordinal() + 1)
                        inforhash = DEFAULT_INFOHASH;
                    else {
                        inforhash = getStringValueOfField(
                                fields[DownloadEnum.IH.ordinal()].toUpperCase(),
                                DEFAULT_INFOHASH);
                    }
                    outputKey.set(inforhash);
                    outputValue.set(formatLog(fields));
                }

                else {
                    fields = value.toString().split(TAB_SEPARATOR, -1);
                    String inforhash = fields[DMInforHashEnum.IH.ordinal()]
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
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

    }

    public static class DownloadFormatReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        private Text outputKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String defaultInfohash = "0\t-1\t-1\t-1\tunknown";
            List<String> outputValues = new LinkedList<String>();
            for (Text value : values) {
                String[] fields = value.toString().split(TAB_SEPARATOR, -1);
                if (fields.length == 5) {
                    defaultInfohash = fields[0] + TAB_SEPARATOR + fields[1]
                            + TAB_SEPARATOR + fields[2] + TAB_SEPARATOR
                            + fields[3] + TAB_SEPARATOR + fields[4];
                }
                else {
                    outputValues.add(value.toString());
                }

            }

            Iterator<String> iterator = outputValues.iterator();
            while (iterator.hasNext()) {
                String value = iterator.next().replace(key.toString(),
                        defaultInfohash);
                outputKey.set(value);
                System.out.println(outputKey);
                context.write(outputKey, NullWritable.get());
            }

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        int nRet = ToolRunner.run(new Configuration(), new DownloadFormat(),
                args);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "log-analytics-format-download");
        job.setJarByClass(DownloadFormat.class);
        HdfsUtil.deleteDir(args[1]);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DownloadFormatMapper.class);
        job.setReducerClass(DownloadFormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        int result = job.waitForCompletion(true) ? 0 : 1;
        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(new Path(args[1]));
        return result;
    }
}