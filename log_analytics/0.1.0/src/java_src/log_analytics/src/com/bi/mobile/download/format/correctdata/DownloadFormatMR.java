/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DownloadFormatMR.java 
 * @Package com.bi.mobile.download.format.correctdata 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-10-12 下午2:49:36 
 * @input:输入日志路径/2013-10-12
 * @output:输出日志路径/2013-10-12
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.mobile.download.format.correctdata;

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
import com.bi.comm.util.MACFormatUtil;
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
import com.bi.mobile.download.format.dataenum.DownLoadEnum;
import com.bi.mobile.download.format.dataenum.DownloadOutIHFormatEnum;
import com.bi.mobile.fbuffer.format.dataenum.FbufferOutIHFormatEnum;

/**
 * @ClassName: DownloadFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-10-12 下午2:49:36
 */
public class DownloadFormatMR extends Configured implements Tool {

    public static class DownloadFormatMappper extends
            Mapper<LongWritable, Text, Text, Text> {
        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO;

        private AbstractDMDAO<Integer, Integer> dmQuDaoRuleDAO;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO;

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

        public String getDownloadOtherFormatStr(String originalData)
                throws Exception {
            StringBuilder downLoadETLSB = new StringBuilder();
            String[] fields = originalData.split(",");
            if (fields.length <= DownLoadEnum.IH.ordinal()) {
                throw new Exception("short");
            }
            String originalDataTranf = originalData.replaceAll(",", "\t");
            String tmpstampInfoStr = fields[DownLoadEnum.TIMESTAMP.ordinal()];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(tmpstampInfoStr);
            // dataId
            String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
            String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

            // hourid
            int hourId = Integer.parseInt(hourIdStr);
            String platInfo = fields[DownLoadEnum.DEV.ordinal()];
            platInfo = PlatTypeFormatUtil.getFormatPlatType(platInfo);

            // platid
            int platId = 0;
            platId = this.dmPlatyRuleDAO.getDMOjb(platInfo);
            String versionInfo = fields[DownLoadEnum.VER.ordinal()];
            long versionId = -0l;
            versionId = IPFormatUtil.ip2long(versionInfo);
            int qudaoId = SidFormatMobileUtil.getSidByEnum(fields,
                    dmQuDaoRuleDAO, DownLoadEnum.class.getName());
            String ipInfoStr = fields[DownLoadEnum.IP.ordinal()];
            long ipLong = 0;
            ipLong = IPFormatUtil.ip2long(ipInfoStr);
            java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                    .getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            String macInfor = FormatMobileUtil.getMac(fields, platId,
                    versionId, DownLoadEnum.class.getName());
            downLoadETLSB.append(dateId + "\t");
            downLoadETLSB.append(hourId + "\t");
            downLoadETLSB.append(platId + "\t");
            downLoadETLSB.append(versionId + "\t");
            downLoadETLSB.append(qudaoId + "\t");
            downLoadETLSB.append(cityId + "\t");
            downLoadETLSB.append(macInfor + "\t");
            downLoadETLSB.append(provinceId + "\t");
            downLoadETLSB.append(ispId + "\t");
            downLoadETLSB.append(originalDataTranf.trim());
            return downLoadETLSB.toString();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originalData = value.toString();
            try {
                if (filePath.toLowerCase().contains("download")) {
                    String downLoadETLStr = this
                            .getDownloadOtherFormatStr(originalData);
                    if (null != downLoadETLStr
                            && !("".equalsIgnoreCase(downLoadETLStr))) {
                        String[] fields = downLoadETLStr.split("\t");
                        String inforhash = fields[DownloadOutIHFormatEnum.IH
                                .ordinal()].toLowerCase();
                        context.write(new Text(inforhash.trim().toLowerCase()),
                                new Text(downLoadETLStr));
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

    public static class DownloadFormatReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String inforhashStr = null;
            List<String> downloadList = new ArrayList<String>();
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(",")) {
                    inforhashStr = value;
                }
                else {

                    downloadList.add(value);
                }
            }

            for (String downloadInfoString : downloadList) {
                String downloadETLValue = "";
                String[] splitDownloadSts = downloadInfoString.split("\t");
                List<String> splitDownloadList = new ArrayList<String>();
                for (String splitDownload : splitDownloadSts) {
                    splitDownloadList.add(splitDownload);
                }
                if (null != inforhashStr) {

                    String[] inforhashStrs = inforhashStr.split(",");
                    splitDownloadList
                            .add(FbufferOutIHFormatEnum.CITY_ID.ordinal(),
                                    inforhashStrs[DMInforHashEnum.CHANNEL_ID
                                            .ordinal()]);
                    splitDownloadList.add(
                            FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
                            inforhashStrs[DMInforHashEnum.MEIDA_ID.ordinal()]);
                    splitDownloadList.add(
                            FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
                            inforhashStrs[DMInforHashEnum.SERIAL_ID.ordinal()]);

                }
                else {

                    splitDownloadList.add(
                            FbufferOutIHFormatEnum.CITY_ID.ordinal(), "-1");
                    splitDownloadList.add(
                            FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 1,
                            "-1");
                    splitDownloadList.add(
                            FbufferOutIHFormatEnum.PROVINCE_ID.ordinal() + 2,
                            "-1");

                }
                for (int i = 0; i < splitDownloadList.size(); i++) {
                    downloadETLValue += splitDownloadList.get(i);
                    if (i < splitDownloadList.size()) {
                        downloadETLValue += "\t";
                    }
                }
                context.write(new Text(downloadETLValue), NullWritable.get());
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
        int nRet = ToolRunner.run(new Configuration(), new DownloadFormatMR(),
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
        job.setJarByClass(com.bi.mobile.download.format.correctdata.DownloadFormatMR.class);
        job.setMapperClass(com.bi.mobile.download.format.correctdata.DownloadFormatMR.DownloadFormatMappper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(com.bi.mobile.download.format.correctdata.DownloadFormatMR.DownloadFormatReducer.class);
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
