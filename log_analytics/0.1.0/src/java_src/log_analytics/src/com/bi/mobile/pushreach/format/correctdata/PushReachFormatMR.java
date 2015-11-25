package com.bi.mobile.pushreach.format.correctdata;

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
import com.bi.comm.util.CommonConstant;
import com.bi.comm.util.HdfsUtil;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.bi.mobile.comm.dm.pojo.dao.DMQuDaoRuleDAOImpl;
import com.bi.mobile.comm.util.FormatMobileUtil;
import com.bi.mobile.comm.util.SidFormatMobileUtil;
import com.bi.mobile.pushreach.format.dataenum.PushReachEnum;

public class PushReachFormatMR extends Configured implements Tool {
    public static class PushReachFormatMappper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

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
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);

        }

        public String getPushReachFormatStr(String originalData)
                throws Exception {

            StringBuilder pushReachETLSB = new StringBuilder();
            String[] fields = originalData.split(",");
            if (fields.length <= PushReachEnum.OK.ordinal()) {
                throw new Exception("short");

            }
            String originalDataTranf = originalData.replaceAll(",", "\t");
            String timestampInfoStr = fields[PushReachEnum.TIMESTAMP.ordinal()];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(timestampInfoStr);
            // dataId
            String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
            String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);
            // hourid
            int hourId = Integer.parseInt(hourIdStr);
            // 获取设备类型
            String platInfo = fields[PushReachEnum.DEV.ordinal()];
            platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);
            // platid
            int platId = 0;
            platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
            String versionInfo = fields[PushReachEnum.VER.ordinal()];
            // versionId
            long versionId = -0l;
            versionId = IPFormatUtil.ip2long(versionInfo);
            int qudaoId = SidFormatMobileUtil.getSidByEnum(fields,
                    dmQuDaoRuleDAO, PushReachEnum.class.getName());
            // ipinfo
            String ipInfoStr = fields[PushReachEnum.IP.ordinal()];
            long ipLong = 0;
            ipLong = IPFormatUtil.ip2long(ipInfoStr);
            java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                    .getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            // mac地址
            String macInfor = FormatMobileUtil.getMac(fields, platId,
                    versionId, PushReachEnum.class.getName());
            // 提取前两个版本信息
            String versionFormatStr = IPFormatUtil.ipFormat(versionInfo);
            // 提取消息类型(messagetype)
            String messgeTypeStr = FormatMobileUtil.messageFormatMobileUtil(
                    fields, PushReachEnum.class.getName());
            // 消息是否展示成功（ok）
            String okStr = FormatMobileUtil.dealWithOk(fields,
                    PushReachEnum.class.getName());
            pushReachETLSB.append(dateId + "\t");
            pushReachETLSB.append(hourId + "\t");
            pushReachETLSB.append(platId + "\t");
            pushReachETLSB.append(versionId + "\t");
            pushReachETLSB.append(qudaoId + "\t");
            pushReachETLSB.append(cityId + "\t");
            pushReachETLSB.append(macInfor + "\t");
            pushReachETLSB.append(provinceId + "\t");
            pushReachETLSB.append(ispId + "\t");
            pushReachETLSB.append(versionFormatStr + "\t");
            pushReachETLSB.append(originalDataTranf.trim());
            return pushReachETLSB.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originalData = value.toString();
            try {
                String pushReachETLStr = getPushReachFormatStr(originalData);
                if (null != pushReachETLStr) {
                    context.write(new Text(pushReachETLStr), NullWritable.get());
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
     */
    public static void main(String[] args) throws Exception {
        int nRet = ToolRunner.run(new Configuration(), new PushReachFormatMR(),
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
        job.setJarByClass(PushReachFormatMR.class);
        job.setMapperClass(PushReachFormatMappper.class);
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
