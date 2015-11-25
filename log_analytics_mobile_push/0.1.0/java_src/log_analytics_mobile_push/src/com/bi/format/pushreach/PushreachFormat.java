package com.bi.format.pushreach;

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

import com.bi.common.constant.ConstantEnum;
import com.bi.common.constant.DimFilePath;
import com.bi.common.dimprocess.AbstractDMDAO;
import com.bi.common.dimprocess.DMPlatyRuleDAOImpl;
import com.bi.common.logenum.PushreachEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.PlatTypeFormatUtil;
import com.bi.common.util.StringUtil;
import com.bi.common.util.TimestampFormatUtil;

public class PushreachFormat extends Configured implements Tool {

    private static String SEPARATOR = "\t";

    public static class PushreachFormatMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            dmPlatyRuleDAO.parseDMObj(new File(DimFilePath.CLUSTER_PLAT_PATH));
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
        }

        public String formatLog(String originLog) throws Exception {
            String[] fields = StringUtil.splitLog(originLog, ',');
            // data_id ,hour_id
            String timeStampOrigin = fields[PushreachEnum.TIMESTAMP.ordinal()];
            Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                    .formatTimestamp(timeStampOrigin);
            String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
            String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

            // versionId
            String versionOrigin = fields[PushreachEnum.VER.ordinal()];
            long versionId = 0l;
            versionId = IPFormatUtil.ip2long(versionOrigin);

            // platid
            String platOrigin = fields[PushreachEnum.DEV.ordinal()];
            platOrigin = PlatTypeFormatUtil.getFormatPlatType(platOrigin);
            int platId = 0;
            platId = dmPlatyRuleDAO.getDMOjb(platOrigin);
            platFilte(platId);

            // mac_format
            String macOrigin = fields[PushreachEnum.MAC.ordinal()];
            MACFormatUtil.isCorrectMac(macOrigin);
            String macFormat = MACFormatUtil.macFormatToCorrectStr(macOrigin);

            // ip_format
            String ipOrigin = fields[PushreachEnum.IP.ordinal()];
            String ipFormat = IPFormatUtil.ipFormat(ipOrigin);

            // messagetype
            String messageTypeOrigin = fields[PushreachEnum.MESSAGETYPE
                    .ordinal()];
            long messageType = Long.parseLong(messageTypeOrigin);

            // ok
            String okOrigin = fields[PushreachEnum.OK.ordinal()];
            int ok = Integer.parseInt(okOrigin);

            StringBuilder formatLog = new StringBuilder();
            formatLog.append(dateId);
            formatLog.append(SEPARATOR);
            formatLog.append(hourId);
            formatLog.append(SEPARATOR);
            formatLog.append(platId);
            formatLog.append(SEPARATOR);
            formatLog.append(versionId);
            formatLog.append(SEPARATOR);
            formatLog.append(macFormat);
            formatLog.append(SEPARATOR);
            formatLog.append(ipFormat);
            formatLog.append(SEPARATOR);
            formatLog.append(messageType);
            formatLog.append(SEPARATOR);
            formatLog.append(timeStampOrigin);
            formatLog.append(SEPARATOR);
            formatLog.append(ok);
            return formatLog.toString();

        }

        private boolean lengthMeet(int length) {

            return length >= PushreachEnum.MESSAGETYPE.ordinal() + 1;
        }

        private void platFilte(int platId) throws Exception {
            boolean platOk = (platId == 5 || platId == 6);
            if (!platOk)
                throw new Exception("plat not ok");

        }

        //
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originLog = value.toString();
            if (!lengthMeet(StringUtil.splitLog(originLog, ',').length)) {
                multipleOutputs.write(new Text("short\t" + originLog),
                        NullWritable.get(), "_error/part");
                return;
            }
            String formatLog;
            try {
                formatLog = formatLog(originLog);
                context.write(new Text(formatLog), NullWritable.get());
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                multipleOutputs.write(new Text(e.getMessage() + "\t"
                        + originLog), NullWritable.get(), "_error/part");
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

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new ConfigFileCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new PushreachFormat(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "pushreach-pushreach-format");
        job.setJarByClass(PushreachFormat.class);
        HdfsUtil.deleteDir(args[1]);
        for (Path path : HdfsUtil.listPaths(args[0])) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PushreachFormatMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}