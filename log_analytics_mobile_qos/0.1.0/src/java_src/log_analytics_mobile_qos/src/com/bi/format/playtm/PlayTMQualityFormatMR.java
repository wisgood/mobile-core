/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayTMQualityFormatMR.java 
 * @Package com.bi.format.playtm 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-26 下午5:22:56 
 * @input:输入日志路径/2013-8-26
 * @output:输出日志路径/2013-8-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.format.playtm;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.constant.DimensionConstant;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.common.logenum.PlayFailEnum;
import com.bi.common.logenum.PlayTMEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: PlayTMQualityFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-26 下午5:22:56
 */
public class PlayTMQualityFormatMR extends Configured implements Tool {

    public static class PlayTMQualityFormatMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private DMPlatyRuleDAOImpl<String, Integer> dmPlatyRuleDAO = null;

        private DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private AbstractDMDAO<String, Map<ConstantEnum, String>> dmServerdebugRuleDAO = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmServerdebugRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();
            dmPlatyRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_PLAT_PATH));
            dmIPRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_IPTABLE_PATH));
            dmServerdebugRuleDAO.parseDMObj(new File(
                    DimFilePath.CLUSTER_SERVER_PATH));
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            StringBuilder playTMETLSB = new StringBuilder();
            String[] splitSts = DataFormatUtils.split(line,
                    DataFormatUtils.COMMA_SEPARATOR, 0);

            try {

                if (splitSts.length < PlayTMEnum.TU.ordinal()) {
                    throw new Exception(" the length of fbuffer log less than "
                            + PlayTMEnum.PTYPE.ordinal());
                }
                splitSts = SpecialVersionRecomposeFormatMobileUtil
                        .recomposeBySpecialVersionIndex(splitSts,
                                PlayTMEnum.class.getName());
                String originalDataTranf = org.apache.commons.lang.StringUtils
                        .join(splitSts, "\t");

                String tmpstampInfoStr = splitSts[PlayTMEnum.TIMESTAMP
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(tmpstampInfoStr);

                // dataId
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

                // hourid
                int hourId = Integer.parseInt(hourIdStr);

                String platInfo = splitSts[PlayTMEnum.DEV.ordinal()];
                platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

                // platid
                int platId = 0;
                platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);

                String versionInfo = splitSts[PlayTMEnum.VER.ordinal()];

                long versionId = 0l;
                versionId = IPFormatUtil.ip2long(versionInfo);

                // ipinfo
                String ipInfoStr = splitSts[PlayTMEnum.IP.ordinal()];
                long ipLong = 0;
                ipLong = IPFormatUtil.ip2long(ipInfoStr);

                java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                // String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // mac地址
                String macInfoStr = splitSts[PlayTMEnum.MAC.ordinal()];
                MACFormatUtil.isCorrectMac(macInfoStr);
                String macInfor = MACFormatUtil
                        .macFormatToCorrectStr(macInfoStr);
                long vtmlong = DataFormatUtils.parseDoubleToLong(splitSts,
                        PlayTMEnum.class.getName(), PlayTMEnum.VTM.toString());
                long pnlong = DataFormatUtils.parseDoubleToLong(splitSts,
                        PlayTMEnum.class.getName(), PlayTMEnum.PN.toString());
                long tulong = DataFormatUtils.parseDoubleToLong(splitSts,
                        PlayTMEnum.class.getName(), PlayTMEnum.TU.toString());

                String ipFormatStr = IPFormatUtil.ipFormat(ipInfoStr);

                DataFormatUtils
                        .filerNoNumber(splitSts[PlayTMEnum.NT.ordinal()]);
                int nettype = Integer
                        .parseInt(splitSts[PlayTMEnum.NT.ordinal()]);
                // server_id
                long serverId = 0;
                serverId = IPFormatUtil.ip2long(splitSts[PlayFailEnum.SERVERIP
                        .ordinal()]);
                Map<ConstantEnum, String> serverdebugMap = dmServerdebugRuleDAO
                        .getDMOjb(serverId + "");
                serverId = Long.parseLong(serverdebugMap
                        .get(ConstantEnum.SERVER_ID));
                playTMETLSB.append(dateId + "\t");
                playTMETLSB.append(hourId + "\t");
                playTMETLSB.append(platId + "\t");
                playTMETLSB.append(versionId + "\t");
                playTMETLSB.append(provinceId + "\t");
                playTMETLSB.append(ispId + "\t");
                playTMETLSB.append(DimensionConstant.TOTTAL_DEFAULT_VALUE
                        + "\t");
                playTMETLSB.append(nettype + "\t");
                playTMETLSB.append(macInfor + "\t");
                playTMETLSB.append(ipFormatStr + "\t");
                playTMETLSB.append(vtmlong + "\t");
                playTMETLSB.append(pnlong + "\t");
                playTMETLSB.append(tulong + "\t");

                context.write(new Text(playTMETLSB.toString()),
                        NullWritable.get());

                if (splitSts.length > PlayTMEnum.CL.ordinal()) {
                    playTMETLSB
                            .append(splitSts[PlayTMEnum.CL.ordinal()] + "\t");

                    multipleOutputs.write(new Text(playTMETLSB.toString()),
                            NullWritable.get(),
                            CommonConstant.CL_FORMAT_OUTPUT_DIR);

                }

            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(e.getMessage()
                        + DataFormatUtils.TAB_SEPARATOR + line.toString()),
                        NullWritable.get(),
                        CommonConstant.ERROR_FORMAT_DIR);

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new PlayTMQualityFormatMR(), args);
        System.out.println(res);

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
        for (int i = 0; i < args.length; i++) {

            System.out.println(i + ":" + args[i]);
        }
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(PlayTMQualityFormatMR.class);
        job.setMapperClass(PlayTMQualityFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_playTMQualityFormatMR_" + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        System.out.println(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
        System.out.println(CommonConstant.REDUCE_NUM + ":" + reduceNum);
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
