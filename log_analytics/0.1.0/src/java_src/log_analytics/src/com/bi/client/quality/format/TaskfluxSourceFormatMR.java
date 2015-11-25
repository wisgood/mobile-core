/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayBufferingFormatMR.java 
 * @Package com.bi.clientquality.format 
 * @Description: 用一句话描述该文件做什么
 * @author niewf
 * @date Aug 29, 2013 8:40:53 PM 
 */
package com.bi.client.quality.format;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.client.quality.enums.TaskfluxSourceEnum;
import com.bi.client.quality.enums.TaskfluxSourceFormatEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * @ClassName: TaskfluxSourceFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author niewf
 * @date Sep 2, 2013 10:41:50 PM
 */
public class TaskfluxSourceFormatMR {

    public static class TaskfluxSourceFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(TaskfluxSourceFormatMapper.class.getName());

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            logger.info("TaskfluxSourceETLMap rummode is cluster!");
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";

            try {

                String timeStampOrigin = fields[TaskfluxSourceEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[TaskfluxSourceEnum.VV.ordinal()];
                String macOrigin = fields[TaskfluxSourceEnum.MAC.ordinal()];
                String clientIPOrigin = fields[TaskfluxSourceEnum.CLIENTIP.ordinal()];
                String msfOrigin = fields[TaskfluxSourceEnum.MSF.ordinal()];
                String hmsfOrigin = fields[TaskfluxSourceEnum.HMSF.ordinal()];
                String nrfOrigin = fields[TaskfluxSourceEnum.NRF.ordinal()];
                // String infohashOrigin = fields[TaskfluxSourceEnum.IH.ordinal()];

                // dateid,hourid
                Map<ConstantEnum, String> timeStampMap = TimestampFormatUtil
                        .formatTimestamp(timeStampOrigin);

                String dateId = timeStampMap.get(ConstantEnum.DATE_ID);
                String hourId = timeStampMap.get(ConstantEnum.HOUR_ID);

                // versionId
                long versionId = 0l;
                versionId = IPFormatUtil.ip2long(versionOrigin);
                
                String macFormat = validateMac(macOrigin);

                long ip = Long.parseLong(clientIPOrigin);
                Map<ConstantEnum, String> ipRuleMap = dmIPRuleDAO.getDMOjb(ip);
                String provenceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                String ispId = ipRuleMap.get(ConstantEnum.ISP_ID);

                // halt count and halt time
                String msfFormat = validNumber(msfOrigin);
                String hmsfFormat = validNumber(hmsfOrigin);
                String nrfFormat = validNumber(nrfOrigin);

                StringBuilder formatLog = new StringBuilder();
                formatLog.append(dateId);
                formatLog.append("\t");
                formatLog.append(hourId);
                formatLog.append("\t");
                formatLog.append(versionId);
                formatLog.append("\t");
                formatLog.append(provenceId);
                formatLog.append("\t");
                formatLog.append(cityId);
                formatLog.append("\t");
                formatLog.append(ispId);
                formatLog.append("\t");
                formatLog.append(macFormat);
                formatLog.append("\t");
                formatLog.append(msfFormat);
                formatLog.append("\t");
                formatLog.append(hmsfFormat);
                formatLog.append("\t");
                formatLog.append(nrfFormat);
                formatLog.append("\t");
                formatLog.append(originLog);
                return formatLog.toString();

            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                logger.error("error originalData:" + fields);
                logger.error(e.getMessage(), e.getCause());
                return empty;
            }
        }
        
        /**
         * @Title: validateMac
         * @Description: 这里用一句话描述这个方法的作用
         * @Auther: niewf
         * @Date: Sep 3, 2013 11:55:29 PM
         */
        private String validateMac(String macOrigin) {
            String result = "000000000000";
            if(macOrigin.compareToIgnoreCase("undef")!=0 ){
                result = macOrigin.toUpperCase();
            }
            return result;
        }

        private String validNumber(String numOrigin) {
            String result = "-1";
            long num = 0;
            try {
                num = Long.parseLong(numOrigin);
                if (num >= 0) {
                    result = numOrigin;
                }
            }
            catch(Exception e) {
                System.out.println("This is not a Number, " + numOrigin);
            }
            return result;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String originLog = value.toString().replaceAll(",", "\t");
            if (!lengthMeet(originLog.length())) {
                return;

            }
            String formatLog = logFormat(originLog);
            if (null != formatLog && !("".equalsIgnoreCase(formatLog))) {
                context.write(
                        new Text(
                                formatLog.split("\t")[TaskfluxSourceFormatEnum.TIMESTAMP
                                        .ordinal()]), new Text(formatLog));
            }
        }

        private boolean lengthMeet(int length) {

            return length >= TaskfluxSourceEnum.ISP.ordinal() + 1;
        }

    }

    public static class TaskfluxSourceFormatReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text());
            }

        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(TaskfluxSourceFormatMR.class);
        job.setJobName("TaskfluxSourceFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/taskflux_source"));
        FileOutputFormat.setOutputPath(job, new Path("output/taskflux_source"));
        job.setMapperClass(TaskfluxSourceFormatMapper.class);
        job.setReducerClass(TaskfluxSourceFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
