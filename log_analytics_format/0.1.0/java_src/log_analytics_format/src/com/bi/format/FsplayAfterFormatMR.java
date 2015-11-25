/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsplayAfterFormatMR.java 
 * @Package com.bi.format 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-26 上午11:30:35 
 * @input:输入日志路径/2013-11-26
 * @output:输出日志路径/2013-11-26
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dm.pojo.DMInforHashEnum;
import com.bi.common.logenum.FormatFsplayAfterNOJoinHasnEnum;
import com.bi.common.logenum.FsplayAfterEnum;
import com.bi.common.util.DMIPRuleDAOImpl;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.StringFormatUtil;
import com.bi.common.util.TimestampFormatNewUtil;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * @ClassName: FsplayAfterFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-26 上午11:30:35
 */
public class FsplayAfterFormatMR extends Configured implements Tool {

    public static class FsplayAfterFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private MultipleOutputs<Text, Text> multipleOutputs;

        private String dateId = null;

        private String filePath;

        private Text keyText = null;

        private Text valueText = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
            dateId = context.getConfiguration().get("dateid");
            keyText = new Text();
            valueText = new Text();
        }

        private String getFsplayAfterStr(String originalData) throws Exception {
            StringBuilder fsplayAfterStr = new StringBuilder();

            String[] splitSts = StringFormatUtil.splitLog(originalData,
                    StringFormatUtil.COMMA_SEPARATOR);
            int fieldLength = splitSts.length;
            String tmpstampInfoStr = splitSts[FsplayAfterEnum.TIMESTAMP
                    .ordinal()];
            // date and hour
            String dateIdAndHourIdStr = TimestampFormatNewUtil.formatTimestamp(
                    tmpstampInfoStr, dateId);
            // ipinfo
            String ipInfoStr = splitSts[FsplayAfterEnum.IP.ordinal()];
            long ipLong = 0;
            ipLong = IPFormatUtil.ip2long(ipInfoStr);
            java.util.Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO
                    .getDMOjb(ipLong);
            String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
            String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
            String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);
            // mac地址
            String macInfoStr = splitSts[FsplayAfterEnum.MAC.ordinal()];
            macInfoStr = MACFormatUtil.getCorrectMac(macInfoStr);
            int platId = 2;
            String uid = "0";
            String fck = "0";
            String infohashId = "0";
            if (splitSts.length > FsplayAfterEnum.IH.ordinal()) {
                infohashId = splitSts[FsplayAfterEnum.IH.ordinal()]
                        .toUpperCase();
            }
            if (splitSts.length > FsplayAfterEnum.FCK.ordinal()) {
                fck = splitSts[FsplayAfterEnum.FCK.ordinal()];
            }
            if (splitSts.length > FsplayAfterEnum.UID.ordinal()) {
                uid = splitSts[FsplayAfterEnum.UID.ordinal()];
            }
            fsplayAfterStr.append(dateIdAndHourIdStr + ""
                    + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(provinceId + ""
                    + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(cityId + "" + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(ispId + "" + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(platId + "" + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(infohashId + ""
                    + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(macInfoStr + ""
                    + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(fck + "" + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(uid + "" + StringFormatUtil.TAB_SEPARATOR);
            fsplayAfterStr.append(TimestampFormatNewUtil.getTimestamp(
                    tmpstampInfoStr, dateId));
            return fsplayAfterStr.toString();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String originalData = value.toString();
            try {
                if (filePath.toLowerCase().contains("fsplay_after")) {
                    String logETLStr = this.getFsplayAfterStr(originalData);
                    String[] fields = logETLStr.split(""
                            + StringFormatUtil.TAB_SEPARATOR);
                    String inforhash = fields[FormatFsplayAfterNOJoinHasnEnum.INFOHASH_ID
                            .ordinal()].toUpperCase();
                    keyText.set(inforhash.trim().toUpperCase());
                    valueText.set(logETLStr);
                    context.write(keyText, valueText);
                }
                else {
                    String[] fields = originalData.split(""
                            + StringFormatUtil.TAB_SEPARATOR);
                    String inforhashlinebycomma = originalData.replaceAll(""
                            + StringFormatUtil.TAB_SEPARATOR,
                            StringFormatUtil.COMMA_SEPARATOR + "");
                    keyText.set(fields[DMInforHashEnum.IH.ordinal()]
                            .toUpperCase().trim());
                    valueText.set(inforhashlinebycomma);
                    context.write(keyText, valueText);
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

    public static class FsplayAfterFormatReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        private Text keyText = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            keyText = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String defaultInfohash = "0\t-1\t-1\t-1\tunknown";
            List<String> fsplayAfterList = new ArrayList<String>();
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(",")) {
                    defaultInfohash = value.replaceAll(
                            StringFormatUtil.COMMA_SEPARATOR + "", ""
                                    + StringFormatUtil.TAB_SEPARATOR);
                }
                else {
                    fsplayAfterList.add(value);
                }
            }

            for (int i = 0; i < fsplayAfterList.size(); i++) {
                String value = fsplayAfterList.get(i).replace(key.toString(),
                        defaultInfohash);
                keyText.set(value);
                context.write(keyText, NullWritable.get());
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
        int nRet = ToolRunner.run(new Configuration(),
                new FsplayAfterFormatMR(), args);
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
        job.setJarByClass(FsplayAfterFormatMR.class);
        job.setMapperClass(FsplayAfterFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FsplayAfterFormatReducer.class);
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
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        int result = job.waitForCompletion(true) ? 0 : 1;
        LzoIndexer lzoIndexer = new LzoIndexer(conf);
        lzoIndexer.index(new Path(outputPathStr));
        return result;
    }

}
