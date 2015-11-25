/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FbufferQualityFormatMR.java 
 * @Package com.bi.format.fbuffer 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-8-26 下午3:13:13 
 * @input:输入日志路径/2013-8-26
 * @output:输出日志路径/2013-8-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.format.fbuffer;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.apache.log4j.Logger;

import com.bi.common.constant.CommonConstant;
import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimensionConstant;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.dm.pojo.dao.DMServerInfoRuleDAOImpl;
import com.bi.common.logenum.FbufferEnum;
import com.bi.common.util.DataFormatUtils;
import com.bi.common.util.FormatMobileUtil;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.SpecialVersionRecomposeFormatMobileUtil;
import com.bi.common.util.TimestampFormatUtil;

/**
 * @ClassName: FbufferQualityFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-8-26 下午3:13:13
 */
public class FbufferQualityFormatMR extends Configured implements Tool {

    public static class FbufferQualityFormatMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {
        private static Logger logger = Logger
                .getLogger(FbufferQualityFormatMapper.class);

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>> dmServerdebugRuleDAO = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmServerdebugRuleDAO = new DMServerInfoRuleDAOImpl<String, Map<ConstantEnum, String>>();
            dmPlatyRuleDAO.parseDMObj(new File(ConstantEnum.DM_MOBILE_PLAT
                    .name().toLowerCase()));
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
            dmServerdebugRuleDAO.parseDMObj(new File(
                    ConstantEnum.DM_MOBILE_SERVER.name().toLowerCase()));
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {
            String originLog = value.toString().replaceAll(",", "\t");
            String[] fields = DataFormatUtils.split(originLog,
                    DataFormatUtils.TAB_SEPARATOR, 0);
            try {
                if (fields.length <= FbufferEnum.DRATE.ordinal()) {

                    throw new Exception(" the length of fbuffer log less than "
                            + FbufferEnum.SID.ordinal());
                }

                fields = SpecialVersionRecomposeFormatMobileUtil
                        .recomposeBySpecialVersionIndex(fields,
                                FbufferEnum.class.getName());
                String timeStampOrigin = fields[FbufferEnum.TIMESTAMP.ordinal()];
                String versionOrigin = fields[FbufferEnum.VER.ordinal()];
                String platOrigin = fields[FbufferEnum.DEV.ordinal()];
                String macOrigin = fields[FbufferEnum.MAC.ordinal()];
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
                       String macFormat = FormatMobileUtil.getMac(fields, platId,
                        versionId, FbufferEnum.class.getName());

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
                // net type
                DataFormatUtils.filerNoNumber(fields[FbufferEnum.NT.ordinal()]);
                int nettype = Integer
                        .parseInt(fields[FbufferEnum.NT.ordinal()]);
                // btm_format,bpos_format
                long btmFormat = DataFormatUtils
                        .parseDoubleToLong(fields, FbufferEnum.class.getName(),
                                FbufferEnum.BTM.toString());
                long bposFormat = DataFormatUtils.parseDoubleToLong(fields,
                        FbufferEnum.class.getName(),
                        FbufferEnum.BPOS.toString());
                // long drateFormat = DataFormatUtils.parseDoubleToLong(fields,
                // FbufferEnum.class.getName(),
                // FbufferEnum.DRATE.toString());
                String drateFormatStr = fields[FbufferEnum.DRATE.ordinal()];

                // DATE_ID, HOUR_ID, PLAT_ID, VERSION_ID, PROVICE_ID, ISP_ID,
                // SERVER_ID, NET_TYPE, MAC_FORMAT, IP_FORMAT, OK, BPOS_FORMAT,
                // BTM_FORMAT, DRATE_FORMAT;

                StringBuilder formatLog = new StringBuilder();
                formatLog.append(dateId);
                formatLog.append("\t");
                formatLog.append(hourId);
                formatLog.append("\t");
                formatLog.append(platId);
                formatLog.append("\t");
                formatLog.append(versionId);
                formatLog.append("\t");
                formatLog.append(provinceId);
                formatLog.append("\t");
                formatLog.append(ispId);
                formatLog.append("\t");
                formatLog.append(DimensionConstant.TOTTAL_DEFAULT_VALUE);
                formatLog.append("\t");
                formatLog.append(nettype);
                formatLog.append("\t");
                formatLog.append(macFormat);
                formatLog.append("\t");
                formatLog.append(ipFormat);
                formatLog.append("\t");
                formatLog.append(fields[FbufferEnum.OK.ordinal()]);
                formatLog.append("\t");
                formatLog.append(bposFormat);
                formatLog.append("\t");
                formatLog.append(btmFormat);
                formatLog.append("\t");
                // formatLog.append(drateFormat);
                formatLog.append(drateFormatStr);
                // multipleOutputs.write(NullWritable.get(),
                // new Text(formatLog.toString()),
                // CommonConstant.FORMAT_OUTPUT_DIR);
                context.write(new Text(formatLog.toString()),
                        NullWritable.get());

            }
            catch(Exception e) {
                // TODO: handle exception
                // e.printStackTrace();
                multipleOutputs.write(new Text(e.getMessage()
                        + DataFormatUtils.TAB_SEPARATOR + value.toString()),
                        NullWritable.get(), CommonConstant.ERROR_FORMAT_DIR);

            }

        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            multipleOutputs.close();
        }

        private long processBtm(String origin) {
            double result = Double.valueOf(origin);
            return (long) result;
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new FbufferQualityFormatMR(), args);
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
        job.setJarByClass(FbufferQualityFormatMR.class);
        job.setMapperClass(FbufferQualityFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String executeDateStr = job.getConfiguration().get(
                CommonConstant.EXECUTE_DATE);
        job.setJobName("mobilequality_fbufferQualityFormatMR_" + executeDateStr);
        String inputPathStr = job.getConfiguration().get(
                CommonConstant.INPUT_PATH);
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                CommonConstant.OUTPUT_PATH);
        System.out.println(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                CommonConstant.REDUCE_NUM, 1);
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
