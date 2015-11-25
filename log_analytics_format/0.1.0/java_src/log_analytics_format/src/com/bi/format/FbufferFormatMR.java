package com.bi.format;

/**   
 * 
 * All rights reserved
 * www.funshion.com
 * @Title: FbufferFormat.java 
 * @Package com.bi.format 
 * @Description: 对download日志进行格式化
 * @author niewf
 * @date 2013-11-26 上午11:30:35 
 * @input:输入日志路径 /dw/logs/mobile/origin/app_fbuffer,/dw/logs/common/dm_common_infohash_new
 * @output:输出日志路径 /dw/logs/format/app_fbuffer
 * @executeCmd:hadoop jar fbufferformat.jar   -files /disk6/datacenter/mobile/conf/ip_table,/disk6/datacenter/mobile/conf/dm_mobile_plat,/disk6/datacenter/mobile/conf/dm_mobile_qudao    
 * @executeCmd:                     -D mapred.reduce.tasks=10  input_path output_path 
 * @inputFormat:    TIMESTAMP, IP, DEV, MAC, VER, NT, IH, SERVERIP, OK, BPOS, BTM, DRATE, SID, NRATE, MSOK, PTYPE, RT, CLIENTIP, CL, MID, EID, VID, VT, FUDID, MESSAGEID, TYPE, LIAN, RTM
 * @ouputFormat:    DATE_ID, HOUR_ID, PROVINCE_ID, CITY_ID, ISP_ID, PLAT_ID, QUDAO_ID, VERSION_ID, SERVER_ID, 
 * @ouputFormat:    CLIENT_IP, MAC_CODE, FUDID, INFOHASH_ID, SERIAL_ID, MEDIA_ID, CHANNEL_ID, MEDIA_NAME, MEDIA_TYPE_ID,
 * @ouputFormat:   TIME_STAMP, NET_TYPE, OK, BUFFER_POS, BUFFER_TIME, DRATE, NRATE, MS_OK, PLAYER_TYPE, CL, MESSAGE_ID, LIAN     
 */

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
import com.bi.common.logenum.FbufferEnum;
import com.bi.common.logenum.FormatFbufferEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.DMQuDaoRuleDAOImpl;
import com.bi.common.util.DefaultUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

public class FbufferFormatMR extends Configured implements Tool {

    public static class FbufferMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private final String VIDEO_TYPE_SMALL = "2"; // vide type, 1:media,
                                                     // 2:video, 3:live

        private Text outputKey = null;

        private Text outputValue = null;

        private String filePath;

        private MultipleOutputs<Text, Text> multipleOutputs;

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
            // this.dmServerInfoRuleDAO = new DMServerInfoRuleDAOImpl<String,
            // Map<ConstantEnum, String>>();
            // this.dmServerInfoRuleDAO.parseDMObj(new
            // File(ConstantEnum.DM_MOBILE_SERVER.name().toLowerCsase()));

            outputKey = new Text();
            outputValue = new Text();

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            multipleOutputs = new MultipleOutputs<Text, Text>(context);

        }

        public String formatLog(String[] originFields) throws Exception {
            try {

                String[] fields = getDefaultFields();
                int length = originFields.length <= FbufferEnum.values().length ? originFields.length
                        : FbufferEnum.values().length;
                System.arraycopy(originFields, 0, fields, 0, length);

                // dateid,hourid
                String timeStamp = fields[FbufferEnum.TIMESTAMP.ordinal()];
                Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                        .formatTimestamp(timeStamp);
                String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
                String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

                // ip_format
                String ipOrigin = fields[FbufferEnum.IP.ordinal()];
                long ipLong = IPFormatUtil.ip2long(ipOrigin);
                Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // platid
                String platOrigin = fields[FbufferEnum.DEV.ordinal()];
                PlatTypeFormatUtil.filterFlash(platOrigin);
                platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
                int platId = 0;
                platId = dmPlatRuleDAO.getDMOjb(platOrigin);

                // versionId
                String versionOrigin = fields[FbufferEnum.VER.ordinal()];
                long versionId = 0L;
                versionId = IPFormatUtil.ip2long(versionOrigin);

                // infohash
                String infohash = getStringValueOfField(
                        fields[FbufferEnum.IH.ordinal()], DefaultUtil.INFOHASH)
                        .toUpperCase();
                String media_type_id = fields[FbufferEnum.VT.ordinal()];
                if (infohash.length() >= 32) {
                    media_type_id = DefaultFieldValueEnum.mediaTypeIdDefault
                            .getValueStr();
                }

                // netTypeId
                int netTypeId = (int) getLongValueOfField(
                        fields[FbufferEnum.NT.ordinal()],
                        Integer.parseInt(DefaultFieldValueEnum.netTypeDefault
                                .getValueStr()));
                if (netTypeId > 3 || netTypeId < -1)
                    netTypeId = Integer
                            .parseInt(DefaultFieldValueEnum.netTypeDefault
                                    .getValueStr());

                // qudaoId
                int qudaoId = (int) getLongValueOfField(
                        fields[FbufferEnum.SID.ordinal()],
                        Integer.parseInt(DefaultFieldValueEnum.qudaoIdDefault
                                .getValueStr()));
                qudaoId = dmQuDaoRuleDAO.getDMOjb(qudaoId);

                // serverID
                String serverIP = null;
                long serverId = 0;
                try {
                    serverIP = IPFormatUtil
                            .ipFormat(fields[FbufferEnum.SERVERIP.ordinal()]);
                }
                catch(Exception e) {
                    serverIP = DefaultFieldValueEnum.IPDefault.getValueStr(); // DEFAULT_IP;
                }
                serverId = IPFormatUtil.ip2long(serverIP);

                String fbufferOk = fields[FbufferEnum.OK.ordinal()];
                // mac
                String mac = MACFormatUtil.getCorrectMac(fields[FbufferEnum.MAC
                        .ordinal()]);
                String fudid = getStringValueOfField(
                        fields[FbufferEnum.FUDID.ordinal()],
                        DefaultFieldValueEnum.fudidDefault.getValueStr());
                // 其他信息
                String bufferPos = fields[FbufferEnum.BPOS.ordinal()];
                String bufferTime = fields[FbufferEnum.BTM.ordinal()];
                String drate = fields[FbufferEnum.DRATE.ordinal()];
                String nrate = fields[FbufferEnum.NRATE.ordinal()];
                String msOk = fields[FbufferEnum.MSOK.ordinal()];
                String playerType = fields[FbufferEnum.PTYPE.ordinal()];
                // messageId
                int messageId = (int) getLongValueOfField(
                        fields[FbufferEnum.MESSAGEID.ordinal()],
                        Integer.parseInt(DefaultFieldValueEnum.messageIdDefault
                                .getValueStr()));
                // clId
                int clId = (int) getLongValueOfField(
                        fields[FbufferEnum.CL.ordinal()],
                        Integer.parseInt(DefaultFieldValueEnum.CLDefault
                                .getValueStr()));
                if (clId > 4 || netTypeId < 1)
                    clId = -1;

                String lian = fields[FbufferEnum.LIAN.ordinal()];
                // String rtm = fields[FbufferEnum.RTM.ordinal()];

                StringBuilder formatLog = new StringBuilder();
                formatLog.append(dateId);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(hourId);
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
                formatLog.append(serverId);
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
                formatLog.append(fbufferOk);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());

                formatLog.append(bufferPos);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(bufferTime);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(drate);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(nrate);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(msOk);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(playerType);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());

                formatLog.append(clId);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(messageId);
                formatLog.append(UtilComstrantsEnum.tabSeparator.getValueStr());
                formatLog.append(lian);

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
            if (null == origin)
                return defaultValue;
            else if ("".equals(origin))
                return defaultValue;
            else
                return origin;

        }

        private String[] getDefaultFields() {
            String[] fields = new String[FbufferEnum.values().length];
            for (int i = 0; i < FbufferEnum.values().length; i++) {
                fields[i] = "";
            }

            fields[0] = "0";// timestamp
            fields[1] = DefaultFieldValueEnum.IPDefault.getValueStr();// ip
            fields[2] = "other";// plat
            fields[3] = DefaultFieldValueEnum.macCodeDefault.getValueStr();// mac
            fields[4] = DefaultFieldValueEnum.IPDefault.getValueStr();// version
            fields[5] = DefaultFieldValueEnum.netTypeDefault.getValueStr();// nt
            fields[6] = DefaultFieldValueEnum.infohashidDefault.getValueStr();// ih
            fields[7] = DefaultFieldValueEnum.IPDefault.getValueStr(); // serverip
            fields[8] = DefaultFieldValueEnum.OKDefault.getValueStr(); // ok
            fields[9] = DefaultFieldValueEnum.bufferPosDefault.getValueStr(); // bpos
            fields[10] = DefaultFieldValueEnum.bufferTimeDefault.getValueStr(); // btm
            fields[11] = DefaultFieldValueEnum.drateDefault.getValueStr(); // drate
            fields[12] = DefaultFieldValueEnum.serialIdDefault.getValueStr(); // sid
            fields[13] = DefaultFieldValueEnum.drateDefault.getValueStr(); // nrate
            fields[14] = DefaultFieldValueEnum.MSOKDefault.getValueStr(); // msok
            fields[15] = DefaultFieldValueEnum.playerTypeDefault.getValueStr(); // ptype
            fields[16] = "-999"; // rt
            fields[17] = DefaultFieldValueEnum.IPDefault.getValueStr(); // iphoneip
            fields[18] = DefaultFieldValueEnum.CLDefault.getValueStr(); // cl
            fields[19] = DefaultFieldValueEnum.mediaIdDefault.getValueStr(); // mid
            fields[20] = "-999"; // eid
            fields[21] = "-999"; // vid
            fields[22] = "-999"; // vt
            fields[23] = DefaultFieldValueEnum.fudidDefault.getValueStr(); // fudid
            fields[24] = DefaultFieldValueEnum.messageIdDefault.getValueStr(); // messageid
            fields[25] = "-999"; // type
            fields[26] = DefaultFieldValueEnum.lianDefault.getValueStr(); // lian

            return fields;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] fields = null;

                if (filePath.toLowerCase().contains("fbuffer")) {
                    fields = value.toString().split(
                            DefaultUtil.COMMA_SEPARATOR, -1);
                    String infohash = fields[FbufferEnum.IH.ordinal()].trim()
                            .toUpperCase();
                    String videoKey = "";

                    if (fields.length > FbufferEnum.VT.ordinal()
                            && VIDEO_TYPE_SMALL
                                    .equalsIgnoreCase(fields[FbufferEnum.VT
                                            .ordinal()])
                            && infohash.length() < 32) {
                        // CID的长度是32位，如果上报Infohash长度大于等于32，则认为其是长视频
                        videoKey = fields[FbufferEnum.VID.ordinal()].trim()
                                .toUpperCase();
                    }
                    else {
                        videoKey = infohash;
                    }
                    outputKey.set(videoKey);
                    outputValue.set(formatLog(fields));
                }
                else if (filePath.toLowerCase().contains("infohash")) {

                    fields = value.toString().split(UtilComstrantsEnum.tabSeparator.getValueStr(),
                            -1);
                    String infohash = fields[DMInfoHashEnum.IH.ordinal()]
                            .trim().toUpperCase();

                    outputKey.set(infohash);
                    outputValue.set(value.toString());

                }
                else {

                    fields = value.toString().split(UtilComstrantsEnum.tabSeparator.getValueStr(),
                            -1);
                    String vid = fields[DMVideoEnum.VIDEO_ID.ordinal()];

                    String videoInfo = DefaultUtil.INFOHASH
                            + UtilComstrantsEnum.tabSeparator.getValueStr() + DefaultUtil.SERIAL_ID
                            + UtilComstrantsEnum.tabSeparator.getValueStr() + value.toString();
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

    public static class FbufferReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] videoInfo = null;
            List<String> fbufferList = new ArrayList<String>();
            for (Text value : values) {
                String[] fields = value.toString().split(
                        UtilComstrantsEnum.tabSeparator.getValueStr(), -1);
                if (fields.length == 5) {
                    videoInfo = value.toString().split(
                            UtilComstrantsEnum.tabSeparator.getValueStr());
                }
                else {
                    fbufferList.add(value.toString());
                }
            }

            for (String fbufferInfoString : fbufferList) {
                String fbufferETLValue = "";
                String[] splitFbufferSts = fbufferInfoString.split("\t");
                List<String> splitFbufferList = new ArrayList<String>();
                for (String splitFbuffer : splitFbufferSts) {
                    splitFbufferList.add(splitFbuffer);
                }
                if (null != videoInfo) {

                    splitFbufferList.set(
                            FormatFbufferEnum.INFOHASH_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.IH.ordinal()]);

                    splitFbufferList.set(FormatFbufferEnum.SERIAL_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.SERIAL_ID.ordinal()]);

                    splitFbufferList.set(FormatFbufferEnum.MEDIA_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.MEDIA_ID.ordinal()]);

                    splitFbufferList.set(
                            FormatFbufferEnum.CHANNEL_ID.ordinal(),
                            videoInfo[DMInfoHashEnum.CHANNEL_ID.ordinal()]);

                    splitFbufferList.set(
                            FormatFbufferEnum.MEDIA_NAME.ordinal(),
                            videoInfo[DMInfoHashEnum.MEDIA_NAME.ordinal()]);
                }

                for (int i = 0; i < splitFbufferList.size(); i++) {
                    if (i > 0) {
                        fbufferETLValue += "\t";
                    }
                    fbufferETLValue += splitFbufferList.get(i);
                }
                context.write(new Text(fbufferETLValue), NullWritable.get());
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
        int nRet = ToolRunner.run(new Configuration(), new FbufferFormatMR(),
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
        job.setJarByClass(FbufferFormatMR.class);
        job.setMapperClass(FbufferMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FbufferReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String jobName = job.getConfiguration().get("jobName", "FbufferFormat");
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
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        int result = job.waitForCompletion(true) ? 0 : 1;
        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(new Path(outputPathStr));

        return result;
    }

}