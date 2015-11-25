/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayTMFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2014-1-7 上午10:04:12 
 * @input:输入日志路径/2014-1-7
 * @output:输出日志路径/2014-1-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DefaultFieldValueEnum;
import com.bi.common.constant.UtilComstrantsEnum;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.DMInfoHashEnum;
import com.bi.common.dm.pojo.DMVideoEnum;
import com.bi.common.logenum.BootStrapEnum;
import com.bi.common.logenum.FormatPlayTMEnum;
import com.bi.common.logenum.PlayTMEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.DefaultUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.TimestampFormatNewUtil;

/**
 * @ClassName: PlayTMFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2014-1-7 上午10:04:12
 */
public class PlayTMFormatMR extends Configured implements Tool {

    public static class PlayTMMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private AbstractDMDAO<String, Integer> dmPlatRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private final String VIDEO_TYPE_LONG = "1";

        private Text outputKey = null;

        private Text outputValue = null;

        private String filePath;

        private MultipleOutputs<Text, Text> multipleOutputs;

        private String dateId = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
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
            outputValue = new Text();

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
            dateId = context.getConfiguration().get("dateid");

        }

        public String formatLog(String[] originFields) throws Exception {

            String[] fields = getDefaultFields();
            int length = originFields.length <= PlayTMEnum.values().length ? originFields.length
                    : PlayTMEnum.values().length;
            System.arraycopy(originFields, 0, fields, 0, length);

            // dateid,hourid
            String tmpstampInfoStr = fields[BootStrapEnum.TIMESTAMP.ordinal()];
            // date and hour
            String dateIdAndHourIdStr = TimestampFormatNewUtil.formatTimestamp(
                    tmpstampInfoStr, dateId);

            // ip_format
            String ipOrigin = fields[PlayTMEnum.IP.ordinal()];
            long ipLong = IPFormatUtil.ip2long(ipOrigin);
            Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

            // platid
            String platOrigin = fields[PlayTMEnum.DEV.ordinal()];
            PlatTypeFormatUtil.filterFlash(platOrigin);
            platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
            int platId = 0;
            platId = dmPlatRuleDAO.getDMOjb(platOrigin);

            // versionId
            String versionOrigin = fields[PlayTMEnum.VER.ordinal()];
            long versionId = IPFormatUtil.ip2long(versionOrigin);
            versionId = IPFormatUtil.ip2long(versionOrigin) == 0 ? DefaultFieldValueEnum.numDefault
                    .getValueInt() : versionId;
            // infohash
            String infohash = fields[PlayTMEnum.IH.ordinal()].toUpperCase();
            String media_type_id = fields[PlayTMEnum.VT.ordinal()];
            if (infohash.length() >= 32) {
                media_type_id = VIDEO_TYPE_LONG;
            }
            media_type_id = getStringValueOfField(media_type_id,
                    DefaultFieldValueEnum.numDefault.getValueStr());

            // netTypeId
            int netTypeId = getIntValueOfField(fields[PlayTMEnum.NT.ordinal()],
                    Integer.parseInt(DefaultFieldValueEnum.netTypeDefault
                            .getValueStr()), false);
            if (netTypeId > 3 || netTypeId < -1)
                netTypeId = Integer
                        .parseInt(DefaultFieldValueEnum.netTypeDefault
                                .getValueStr());

            // qudaoId
            int qudaoId = getIntValueOfField(fields[PlayTMEnum.SID.ordinal()],
                    Integer.parseInt(DefaultFieldValueEnum.qudaoIdDefault
                            .getValueStr()), false);
            qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);

            // mac
            String mac = MACFormatUtil.getCorrectMac(fields[PlayTMEnum.MAC
                    .ordinal()]);
            // fudid
            String fudid = getStringValueOfField(
                    fields[PlayTMEnum.FUDID.ordinal()],
                    DefaultFieldValueEnum.fudidDefault.getValueStr());
            // timeStamp
            String timeStamp = TimestampFormatNewUtil.getTimestamp(
                    tmpstampInfoStr, dateId);
            // 其他信息
            long spos = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.SPOS.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            long epos = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.EPOS.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            long vtm = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.VTM.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    true);
            if (vtm > 1000 * 60 * 60 * 24l) {
                vtm = DefaultFieldValueEnum.numIndexDefault.getValueInt();
            }
            long pn = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.PN.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            long tu = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.TU.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            if (tu > 1000 * 60 * 60 * 24l) {
                tu = DefaultFieldValueEnum.numIndexDefault.getValueInt();
            }
            int playerType = getIntValueOfField(
                    fields[PlayTMEnum.PTYPE.ordinal()],
                    DefaultFieldValueEnum.numDefault.getValueInt(), false);

            if (playerType > 1 || playerType < 0) {
                playerType = DefaultFieldValueEnum.numDefault.getValueInt();
            }

            int pbre = getIntValueOfField(fields[PlayTMEnum.PBRE.ordinal()],
                    DefaultFieldValueEnum.pbreDefault.getValueInt(), true);
            String rt = fields[PlayTMEnum.RT.ordinal()];
            int cl = getIntValueOfField(fields[PlayTMEnum.CL.ordinal()],
                    DefaultFieldValueEnum.numDefault.getValueInt(), false);
            long sn = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.SN.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            long st = (long) getDoubleValueOfField(
                    fields[PlayTMEnum.ST.ordinal()],
                    DefaultFieldValueEnum.numIndexDefault.getValueInt(), true,
                    false);
            if (st > 1000 * 60 * 60 * 24l) {
                st = DefaultFieldValueEnum.numIndexDefault.getValueInt();
            }
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
            formatLog.append(infohash);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(DefaultUtil.SERIAL_ID);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(DefaultUtil.MEDIA_ID);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(DefaultUtil.CHANNELL_ID);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(DefaultUtil.MEDIA_NAME);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(media_type_id);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(timeStamp);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(netTypeId);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(spos);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(epos);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(vtm);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(pn);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(tu);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(playerType);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(pbre);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());

            formatLog.append(rt);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(cl);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(sn);
            formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
            formatLog.append(st);
            return formatLog.toString();

        }

        private double getDoubleValueOfField(String origin, int defaultValue,
                boolean isNoNegtive, boolean isValidatePlayTm) {

            try {
                double resultValue = Double.parseDouble(origin);
                if (isNoNegtive && !isValidatePlayTm) {
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

        private String[] getDefaultFields() {
            String[] fields = new String[PlayTMEnum.values().length];
            for (int i = 0; i < PlayTMEnum.values().length; i++) {
                fields[i] = "";
            }
            // TIMESTAMP, IP, DEV, MAC, VER, NT, IH, SPOS, EPOS, VTM, SID,
            // PN,TU,PTYPE,PBRE,RT,IPHONEIP,CL,FUDID,VT,TYPE,MID,EID,SN,ST
            fields[PlayTMEnum.TIMESTAMP.ordinal()] = "0";
            fields[PlayTMEnum.IP.ordinal()] = DefaultFieldValueEnum.IPDefault
                    .getValueStr();
            fields[PlayTMEnum.DEV.ordinal()] = "other";
            fields[PlayTMEnum.MAC.ordinal()] = DefaultFieldValueEnum.macCodeDefault
                    .getValueStr();
            fields[PlayTMEnum.VER.ordinal()] = DefaultFieldValueEnum.versionIdDefault
                    .getValueStr();
            fields[PlayTMEnum.NT.ordinal()] = DefaultFieldValueEnum.netTypeDefault
                    .getValueStr();
            fields[PlayTMEnum.IH.ordinal()] = DefaultFieldValueEnum.infohashidDefault
                    .getValueStr();
            fields[PlayTMEnum.SPOS.ordinal()] = "0";
            fields[PlayTMEnum.EPOS.ordinal()] = "0";
            fields[PlayTMEnum.VTM.ordinal()] = "0";
            fields[PlayTMEnum.SID.ordinal()] = "-999";
            fields[PlayTMEnum.PN.ordinal()] = "0";
            fields[PlayTMEnum.TU.ordinal()] = "0";
            fields[PlayTMEnum.PTYPE.ordinal()] = "0";
            fields[PlayTMEnum.PBRE.ordinal()] = DefaultFieldValueEnum.pbreDefault
                    .getValueStr();
            fields[PlayTMEnum.RT.ordinal()] = "";
            fields[PlayTMEnum.IPHONEIP.ordinal()] = "";
            fields[PlayTMEnum.CL.ordinal()] = "-999";
            fields[PlayTMEnum.FUDID.ordinal()] = DefaultFieldValueEnum.fudidDefault
                    .getValueStr(); // eid
            fields[PlayTMEnum.SN.ordinal()] = "0"; // type
            fields[PlayTMEnum.ST.ordinal()] = "0"; // lian

            return fields;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = null;

                if (filePath.toLowerCase().contains("playtm")) {
                    fields = value.toString().split(
                            DefaultUtil.COMMA_SEPARATOR, -1);
                    String infohash = fields[PlayTMEnum.IH.ordinal()].trim()
                            .toUpperCase();
                    String videoKey = infohash;
                    outputKey.set(videoKey);
                    outputValue.set(formatLog(fields));
                }
                else if (filePath.toLowerCase().contains("infohash")) {
                    fields = value.toString().split(DefaultUtil.TAB_SEPARATOR,
                            -1);
                    String infohash = fields[DMInfoHashEnum.IH.ordinal()]
                            .trim().toUpperCase();
                    outputKey.set(infohash);
                    outputValue.set(value.toString());
                }
                else {
                    fields = value.toString().split(DefaultUtil.TAB_SEPARATOR,
                            -1);
                    String vid = fields[DMVideoEnum.VIDEO_ID.ordinal()];
                    String videoInfo = DefaultUtil.INFOHASH
                            + DefaultUtil.TAB_SEPARATOR + DefaultUtil.SERIAL_ID
                            + DefaultUtil.TAB_SEPARATOR + value.toString();
                    outputKey.set(vid);
                    outputValue.set(videoInfo);
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

    public static class PlayTMReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] videoInfo = null;
            List<String> playtmList = new ArrayList<String>();
            for (Text value : values) {
                String[] fields = value.toString().split(
                        DefaultUtil.TAB_SEPARATOR, -1);
                if (fields.length == 5) {
                    videoInfo = value.toString().split(
                            DefaultUtil.TAB_SEPARATOR);
                }
                else {
                    playtmList.add(value.toString());
                }
            }

            for (String playtmInfoString : playtmList) {
                String playtmETLValue = "";
                String[] splitPlayTMSts = playtmInfoString.split("\t");
                List<String> splitPlayTMList = new ArrayList<String>();
                for (String splitPlayTM : splitPlayTMSts) {
                    splitPlayTMList.add(splitPlayTM);
                }
                if (null != videoInfo) {
                    splitPlayTMList.set(FormatPlayTMEnum.INFOHASH_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.IH.ordinal()]);

                    splitPlayTMList.set(FormatPlayTMEnum.SERIAL_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.SERIAL_ID.ordinal()]);

                    splitPlayTMList.set(FormatPlayTMEnum.MEDIA_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.MEDIA_ID.ordinal()]);

                    splitPlayTMList.set(FormatPlayTMEnum.CHANNEL_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.CHANNEL_ID.ordinal()]);

                    splitPlayTMList.set(FormatPlayTMEnum.MEDIA_NAME.ordinal(),
                            videoInfo[DMInfoHashEnum.MEDIA_NAME.ordinal()]);
                }
                for (int i = 0; i < splitPlayTMList.size(); i++) {
                    if (i > 0) {
                        playtmETLValue += "\t";
                    }
                    playtmETLValue += splitPlayTMList.get(i);
                }
                context.write(new Text(playtmETLValue), NullWritable.get());
            }
        }

    }

    /**
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int nRet = ToolRunner.run(new Configuration(), new PlayTMFormatMR(),
                args);
        System.out.println(nRet);
    }

    /**
     * 
     * 
     * @Title: run
     * @Description: 这里用一句话描述这个方法的作用
     * @Auther: niewf
     * @Date: Nov 28, 2013 4:21:30 PM
     */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(PlayTMFormatMR.class);
        job.setMapperClass(PlayTMMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(PlayTMReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String jobName = job.getConfiguration().get("jobName", "PlayTMFormat");
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        HdfsUtil.deleteDir(outputPathStr);
        System.out.println(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 8);
        System.out.println(CommonConstant.REDUCE_NUM + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job.getConfiguration().getInt(
                CommonConstant.IS_INPUTFORMATLZOCOMPRESS, 1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        int result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }

}
