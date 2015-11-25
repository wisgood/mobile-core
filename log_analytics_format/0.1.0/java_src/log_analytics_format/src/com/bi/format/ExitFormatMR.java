/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ExitFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2014-1-6 下午5:39:33 
 * @input:输入日志路径/2014-1-6
 * @output:输出日志路径/2014-1-6
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.logenum.ExitEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.DefaultUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.common.util.TimestampFormatNewUtil;

/**
 * @ClassName: ExitFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2014-1-6 下午5:39:33
 */
public class ExitFormatMR extends Configured implements Tool {

    public static class ExitFormatMappper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private AbstractDMDAO<String, Integer> dmPlatRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

        private Text outputKey = null;

        private String dateId = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            dmPlatRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmPlatRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_PLAT
                    .name().toLowerCase()));

            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
            dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            dmQuDaoRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_QUDAO
                    .name().toLowerCase()));
            outputKey = new Text();
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
            dateId = context.getConfiguration().get("dateid");

        }

        private String[] getDefaultFields() {
            String[] fields = new String[ExitEnum.values().length];
            for (int i = 0; i < ExitEnum.values().length; i++) {
                fields[i] = "";
            }
            fields[ExitEnum.TIMESTAMP.ordinal()] = "0";
            fields[ExitEnum.IP.ordinal()] = DefaultFieldValueEnum.IPDefault
                    .getValueStr();
            fields[ExitEnum.DEV.ordinal()] = "other";
            fields[ExitEnum.MAC.ordinal()] = DefaultFieldValueEnum.macCodeDefault
                    .getValueStr();
            fields[ExitEnum.VER.ordinal()] = DefaultFieldValueEnum.versionIdDefault
                    .getValueStr();
            fields[ExitEnum.NT.ordinal()] = DefaultFieldValueEnum.netTypeDefault
                    .getValueStr();
            fields[ExitEnum.USETM.ordinal()] = "0";
            fields[ExitEnum.TN.ordinal()] = "0";
            fields[ExitEnum.SID.ordinal()] = "-999";
            fields[ExitEnum.RT.ordinal()] = "0";
            fields[ExitEnum.IPHONEIP.ordinal()] = DefaultFieldValueEnum.IPDefault
                    .getValueStr();
            fields[ExitEnum.FUDID.ordinal()] = DefaultFieldValueEnum.fudidDefault
                    .getValueStr();
            return fields;
        }

        private String formatLog(String[] originFields) throws Exception {
            originFields = SpecialVersionRecomposeFormatMobileUtil
                    .recomposeBySpecialVersion(originFields,
                            ExitEnum.class.getName());
            String[] fields = getDefaultFields();
            int length = originFields.length <= ExitEnum.values().length ? originFields.length
                    : ExitEnum.values().length;
            System.arraycopy(originFields, 0, fields, 0, length);
            String tmpstampInfoStr = fields[ExitEnum.TIMESTAMP.ordinal()];
            // date and hour
            String dateIdAndHourIdStr = TimestampFormatNewUtil.formatTimestamp(
                    tmpstampInfoStr, dateId);
            // ip_format
            String ipOrigin = fields[ExitEnum.IP.ordinal()];
            long ipLong = IPFormatUtil.ip2long(ipOrigin);
            Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            // platid
            String platOrigin = fields[ExitEnum.DEV.ordinal()];
            PlatTypeFormatUtil.filterFlash(platOrigin);
            platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
            int platId = 0;
            platId = dmPlatRuleDAO.getDMOjb(platOrigin);
            // qudaoId
            int qudaoId = (int) getLongValueOfField(
                    fields[ExitEnum.SID.ordinal()],
                    Integer.parseInt(DefaultFieldValueEnum.qudaoIdDefault
                            .getValueStr()), false, false);
            qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);
            // versionId
            String versionOrigin = fields[ExitEnum.VER.ordinal()];
            long versionId = IPFormatUtil.ip2long(versionOrigin);
            versionId = IPFormatUtil.ip2long(versionOrigin) == 0 ? DefaultFieldValueEnum.numDefault
                    .getValueInt() : versionId;
            ipOrigin = DefaultFieldValueEnum.IPDefault.getValueStr();
            try {
                ipOrigin = IPFormatUtil.ipFormat(fields[ExitEnum.IP.ordinal()]
                        .trim());
            }
            catch(Exception e) {
                // TODO: handle exception
                ipOrigin = DefaultFieldValueEnum.IPDefault.getValueStr();

            }
            // mac
            String mac = MACFormatUtil.getCorrectMac(fields[ExitEnum.MAC
                    .ordinal()]);
            // fudid
            String fudid = getStringValueOfField(
                    fields[ExitEnum.FUDID.ordinal()],
                    DefaultFieldValueEnum.fudidDefault.getValueStr());
            // timeStamp
            String timeStamp = TimestampFormatNewUtil.getTimestamp(
                    tmpstampInfoStr, dateId);
            // netTypeId
            int netTypeId = getIntValueOfField(fields[ExitEnum.NT.ordinal()],
                    Integer.parseInt(DefaultFieldValueEnum.netTypeDefault
                            .getValueStr()), false);
            if (netTypeId > 3 || netTypeId < -1)
                netTypeId = Integer
                        .parseInt(DefaultFieldValueEnum.netTypeDefault
                                .getValueStr());
            // usetm
            long usetm = (long) getDoubleValueOfField(
                    fields[ExitEnum.USETM.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    true);

            if (usetm > 1000 * 60 * 60 * 24l) {
                usetm = DefaultFieldValueEnum.numIndexDefault.getValueInt();
            }

            // tn
            int tn = getIntValueOfField(fields[ExitEnum.TN.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true);
            String rt = fields[ExitEnum.RT.ordinal()];

            StringBuilder formatLog = new StringBuilder();
            formatLog.append(dateIdAndHourIdStr);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(provinceId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(cityId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(ispId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(platId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(qudaoId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(versionId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(ipOrigin);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(mac);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(fudid);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());

            formatLog.append(timeStamp);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(netTypeId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(usetm);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(tn);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(rt);
            return formatLog.toString();

        }

        private long getLongValueOfField(String origin, long defaultValue,
                boolean isNoNegtive, boolean isValidatePlayTm) {
            try {
                long resultValue = Long.parseLong(origin);
                if (isNoNegtive && isValidatePlayTm == false) {
                    return resultValue >= 0 ? resultValue : defaultValue;
                }
                if (isValidatePlayTm) {
                    return resultValue < 1000 * 60 * 60 * 24l ? resultValue
                            : defaultValue;
                }
                return resultValue;
            }
            catch(Exception e) {
                return defaultValue;
            }

        }

        private double getDoubleValueOfField(String origin, int defaultValue,
                boolean isNoNegtive, boolean isValidatePlayTm) {

            try {
                double resultValue = Double.parseDouble(origin);
                if (isNoNegtive && !isValidatePlayTm) {
                    return resultValue >= 0 ? resultValue : defaultValue;
                }
                if (isValidatePlayTm) {
                    return resultValue < (1000 * 60 * 60 * 24l) ? resultValue
                            : defaultValue;
                }
                return resultValue;
            }
            catch(Exception e) {
                return defaultValue;
            }
        }

        private int getIntValueOfField(String origin, int defaultValue,
                boolean isNoNegtive) {
            try {
                int resultValue = Integer.parseInt(origin);
                if (isNoNegtive) {
                    return resultValue >= 0 ? resultValue : defaultValue;
                }
                return resultValue;
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
            else if ("null".equals(origin))
                return defaultValue;
            else
                return origin;

        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            multipleOutputs.close();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originalData = value.toString();
            try {
                String[] fields = value.toString().split(
                        DefaultUtil.COMMA_SEPARATOR, -1);
                String bootStrapETLStr = formatLog(fields);
                if (null != bootStrapETLStr) {
                    outputKey.set(bootStrapETLStr);
                    context.write(outputKey, NullWritable.get());
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                String errorMessage = null == e.getMessage() ? "error" : e
                        .getMessage();
                multipleOutputs.write(new Text(errorMessage + "\t"
                        + originalData), NullWritable.get(), "_error/part");
                return;
            }
        }
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int nRet = ToolRunner
                .run(new Configuration(), new ExitFormatMR(), args);
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(ExitFormatMR.class);
        job.setMapperClass(ExitFormatMappper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        String jobName = job.getConfiguration().get("jobName");
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
        System.out.println(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 0);
        System.out.println(CommonConstant.REDUCE_NUM + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job.getConfiguration().getInt(
                CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.waitForCompletion(true);
        return 0;
    }
}
