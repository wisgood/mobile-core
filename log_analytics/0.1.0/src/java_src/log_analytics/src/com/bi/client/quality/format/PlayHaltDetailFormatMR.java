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

import com.bi.client.quality.enums.PlayHaltDetailEnum;
import com.bi.client.quality.enums.PlayHaltDetailFormatEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * @ClassName: PlayHaltDetailedFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author niewf
 * @date Sep 2, 2013 10:12:41 PM
 */
public class PlayHaltDetailFormatMR {

    public static class PlayHaltDetailFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(PlayHaltDetailFormatMapper.class.getName());

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            logger.info("PlayHaltDetailedETLMap rummode is cluster!");
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";

            try {

                String timeStampOrigin = fields[PlayHaltDetailEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[PlayHaltDetailEnum.VV.ordinal()];
                 String macOrigin = fields[PlayHaltDetailEnum.MAC.ordinal()];
                String clientIPOrigin = fields[PlayHaltDetailEnum.CLIENTIP.ordinal()];
                String hcOrigin = fields[PlayHaltDetailEnum.HC.ordinal()];
                String htaOrigin = fields[PlayHaltDetailEnum.HTA.ordinal()];
                String htOrigin = fields[PlayHaltDetailEnum.HT.ordinal()];
                String dhcOrigin = fields[PlayHaltDetailEnum.DHC.ordinal()];
                String dhtOrigin = fields[PlayHaltDetailEnum.DHT.ordinal()];
                // String infohashOrigin = fields[PlayHaltDetailedEnum.IH.ordinal()];

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
                String hcFormat = validNumber(hcOrigin);
                String htaFormat = validNumber(htaOrigin);
                String htFormat = validHaltType(htOrigin);
                String dhcFormat = validNumber(dhcOrigin);
                String dhtFormat = validNumber(dhtOrigin);

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
                formatLog.append(hcFormat);
                formatLog.append("\t");
                formatLog.append(htaFormat);
                formatLog.append("\t");
                formatLog.append(htFormat);
                formatLog.append("\t");
                formatLog.append(dhcFormat);
                formatLog.append("\t");
                formatLog.append(dhtFormat);
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

        /**
         * @Title: validHaltType
         * @Description: 验证卡类型,(1-边看边下；2-先下后看),失败后置为-1
         * @Auther: niewf
         * @Date: Sep 2, 2013 10:20:23 PM
         */
      
        private String validHaltType(String numOrigin) {
            String result = "-1";
            if(numOrigin.compareToIgnoreCase("1")==0||
                    numOrigin.compareToIgnoreCase("2")==0){
                result = numOrigin;
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
                                formatLog.split("\t")[PlayHaltDetailFormatEnum.TIMESTAMP
                                        .ordinal()]), new Text(formatLog));
            }
        }

        private boolean lengthMeet(int length) {

            return length >= PlayHaltDetailEnum.ISP.ordinal() + 1;
        }

    }

    public static class PlayHaltDetailFormatReducer extends
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
        job.setJarByClass(PlayHaltDetailFormatMR.class);
        job.setJobName("PlayHaltDetailedFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/play_halt_detail"));
        FileOutputFormat.setOutputPath(job, new Path("output/play_halt_detail"));
        job.setMapperClass(PlayHaltDetailFormatMapper.class);
        job.setReducerClass(PlayHaltDetailFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
