/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayTMFormatMR.java 
 * @Package com.bi.mobile.playtm.format.correctdata 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-10-12 下午7:55:13 
 * @input:输入日志路径/2013-10-12
 * @output:输出日志路径/2013-10-12
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.mobile.playtm.format.correctdata;

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

import com.bi.comm.util.CommonConstant;
import com.bi.comm.util.HdfsUtil;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.DMInforHashEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.playtm.format.dataenum.PlayTMEnum;
import com.bi.mobile.playtm.format.dataenum.PlayTMOutIHFormatEnum;
/**
 * @ClassName: PlayTMFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-10-12 下午7:55:13
 */
public class PlayTMFormatMR extends Configured implements Tool {

    public static class PlayTMFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private MultipleOutputs<Text, Text> multipleOutputs;

        private String filePath;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            this.dmQuDaoRuleDAO = new DMQuDaoRuleDAOImpl<Integer, Integer>();
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                    .name().toLowerCase());
            this.dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
            this.dmQuDaoRuleDAO.parseDMObj(new File(
                    ConstantEnum.DM_MOBILE_QUDAO.name().toLowerCase()));
            this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            multipleOutputs = new MultipleOutputs<Text, Text>(context);

        }

        public String getPlayTMOutIHFormatStr(String originalData)
                throws Exception {

            StringBuilder playTMETLSB = new StringBuilder();
            String[] fields = originalData.split(",");
            if (fields.length <= PlayTMEnum.VTM.ordinal()) {
                throw new Exception("short");
            }
            String originalDataTranf = originalData.replaceAll(",", "\t");
            String tmpstampInfoStr = fields[PlayTMEnum.TIMESTAMP.ordinal()];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(tmpstampInfoStr);
            String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
            String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
            int hourId = Integer.parseInt(hourIdStr);
            String platInfo = fields[PlayTMEnum.DEV.ordinal()];
            PlatTypeFormatUtil.filterFlash(platInfo);
            platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
            int platId = 0;
            platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
            String versionInfo = fields[PlayTMEnum.VER.ordinal()];
            long versionId = 0l;
            versionId = IPFormatUtil.ip2long(versionInfo);
            int qudaoId = SidFormatMobileUtil.getSidByEnum(fields,
                    dmQuDaoRuleDAO, PlayTMEnum.class.getName());
            String ipInfoStr = fields[PlayTMEnum.IP.ordinal()];
            long ipLong = 0;
            ipLong = IPFormatUtil.ip2long(ipInfoStr);
            java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                    .getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            String macInfor = FormatMobileUtil.getMac(fields, platId,
                    versionId, PlayTMEnum.class.getName());
            String vtmStr = fields[PlayTMEnum.VTM.ordinal()];
            double vtmDouble = Double.parseDouble(vtmStr);
            long vtmlong = (long) vtmDouble;
            if (!(vtmlong >= 0 && vtmlong < 2147483647)) {
                throw new Exception("播放时长不满足指定范围!");
            }
            playTMETLSB.append(dateId + "\t");
            playTMETLSB.append(hourId + "\t");
            playTMETLSB.append(platId + "\t");
            playTMETLSB.append(versionId + "\t");
            playTMETLSB.append(qudaoId + "\t");
            playTMETLSB.append(cityId + "\t");
            playTMETLSB.append(macInfor + "\t");
            playTMETLSB.append(provinceId + "\t");
            playTMETLSB.append(ispId + "\t");
            playTMETLSB.append(originalDataTranf.trim());
            return playTMETLSB.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originalData = value.toString();
            try {
                if (filePath.toLowerCase().contains("playtm")) {
                    String logETLStr = this
                            .getPlayTMOutIHFormatStr(originalData);
                    if (null != logETLStr && !("".equalsIgnoreCase(logETLStr))) {
                        String[] fields = logETLStr.split("\t");
                        String inforhash = fields[PlayTMOutIHFormatEnum.IH
                                .ordinal()].toLowerCase();
                        context.write(new Text(inforhash.trim().toLowerCase()),
                                new Text(logETLStr));
                    }
                }
                else {
                    String[] fields = originalData.split("\t");
                    String inforhashlinebycomma = originalData.replaceAll("\t",
                            ",");
                    context.write(new Text(fields[DMInforHashEnum.IH.ordinal()]
                            .trim().toLowerCase()), new Text(
                            inforhashlinebycomma.toLowerCase()));
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(null == e.getMessage() ? "error"
                        : e.getMessage()), new Text(originalData),
                        "_error/part");
                return;
            }
        }
    }

    public static class PlayTMFormatReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String inforhashStr = null;
            List<String> playList = new ArrayList<String>();
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(",")) {
                    inforhashStr = value;
                }
                else {

                    playList.add(value);
                }
            }

            for (String playtmInfoString : playList) {
                String playtmETLValue = "";
                String[] splitPlaytmSts = playtmInfoString.split("\t");
                List<String> splitPlaytmList = new ArrayList<String>();
                for (String splitPlaytm : splitPlaytmSts) {
                    splitPlaytmList.add(splitPlaytm);
                }
                if (null != inforhashStr) {
                    String[] inforhashStrs = inforhashStr.split(",");
                    splitPlaytmList
                            .add(PlayTMOutIHFormatEnum.CITY_ID.ordinal(),
                                    inforhashStrs[DMInforHashEnum.CHANNEL_ID
                                            .ordinal()]);
                    splitPlaytmList.add(
                            PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
                            inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
                    splitPlaytmList.add(
                            PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
                            inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);
                }
                else {
                    splitPlaytmList.add(
                            PlayTMOutIHFormatEnum.CITY_ID.ordinal(), "-1");
                    splitPlaytmList.add(
                            PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
                            "-1");
                    splitPlaytmList.add(
                            PlayTMOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
                            "-1");
                }
                for (int i = 0; i < splitPlaytmList.size(); i++) {
                    playtmETLValue += splitPlaytmList.get(i);
                    if (i < splitPlaytmList.size()) {
                        playtmETLValue += "\t";
                    }
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
     * (非 Javadoc)
     * <p>
     * Title: run
     * </p>
     * <p>
     * Description:
     * </p>
     * 
     * @param args
     * @return
     * @throws Exception
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(com.bi.mobile.playtm.format.correctdata.PlayTMFormatMR.class);
        job.setMapperClass(com.bi.mobile.playtm.format.correctdata.PlayTMFormatMR.PlayTMFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(com.bi.mobile.playtm.format.correctdata.PlayTMFormatMR.PlayTMFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
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
