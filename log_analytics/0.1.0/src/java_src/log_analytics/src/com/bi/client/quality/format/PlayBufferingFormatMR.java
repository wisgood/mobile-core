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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.client.quality.enums.PlayBufferingEnum;
import com.bi.client.quality.enums.PlayBufferingFormatEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * @ClassName: PlayBufferingFormatMR
 * @Description: 这里用一句话描述这个类的作用
 * @author niewf
 * @date Aug 29, 2013 8:40:53 PM
 */
public class PlayBufferingFormatMR {

    public static class PlayBufferingFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(PlayBufferingFormatMapper.class.getName());

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            logger.info("PlayBufferingETLMap rummode is cluster!");
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";

            try {

                String timeStampOrigin = fields[PlayBufferingEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[PlayBufferingEnum.VER.ordinal()];
                String macOrigin = fields[PlayBufferingEnum.MAC.ordinal()];
                String clientIPOrigin = fields[PlayBufferingEnum.CLIENTIP.ordinal()];
                String prnOrigin = fields[PlayBufferingEnum.PRN.ordinal()];
                String sdnOrigin = fields[PlayBufferingEnum.SDN.ordinal()];
//                String infohashOrigin = fields[PlayBufferingEnum.IH.ordinal()];
                String okOrigin = fields[PlayBufferingEnum.OK.ordinal()];
                String btOrigin = fields[PlayBufferingEnum.BT.ordinal()];

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

                // peer and seed num
                int prnFormat = processNum(prnOrigin);
                int sdnFormat = processNum(sdnOrigin);
                
                String okFormat = validOK(okOrigin);
                String btFormat = validBufferTime(btOrigin);

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
                formatLog.append(prnFormat);
                formatLog.append("\t");
                formatLog.append(sdnFormat);
                formatLog.append("\t");
                formatLog.append(okFormat);
                formatLog.append("\t");
                formatLog.append(btFormat);
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
         * @ClassName: processNum
         * @Description: 处理SeedNum和PeerNum
         * @author niewf
         * @date Aug 29, 2013 8:40:53 PM
         */
        private int processNum(String origin) {
            Pattern numPattern=Pattern.compile("^([\\d]+)[\\w]*");
            Matcher numMatcher = numPattern.matcher(origin);
            int result = -1;
            
            try {
                if (numMatcher.find()) {
                    String numString = numMatcher.group(1);
                    result = Integer.parseInt(numString);
                }
            }
            catch(NumberFormatException e) {
                result = -1;
            }
            return result;
        }
        
        /**
         * @ClassName: validOK
         * @Description: 验证OK字段，只能为０或１
         * @author niewf
         * @date Aug 29, 2013 8:40:53 PM
         */
        private String validOK(String okOrigin) {
            String result = "-1";
            if (okOrigin.compareToIgnoreCase("1") == 0
                    || okOrigin.compareToIgnoreCase("0") == 0) {
                result = okOrigin;
            }
            return result;
        }
        
        /**
         * @ClassName: validBufferTime
         * @Description: 验证缓冲时间，小于０或无法解析为整型时置为-1
         * @author niewf
         * @date Aug 29, 2013 8:40:53 PM
         */
        private String validBufferTime(String btOrigin){
            String result = "-1";
            long bt = 0;
            try{
                bt = Long.parseLong(btOrigin);
                if(bt >= 0){
                    result = btOrigin;
                }
            }catch (Exception e){
                System.out.println("BT is not a Number, " + btOrigin);
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
                                formatLog.split("\t")[PlayBufferingFormatEnum.TIMESTAMP
                                        .ordinal()]), new Text(formatLog));
            }
        }

        private boolean lengthMeet(int length) {

            return length >= PlayBufferingEnum.ISP.ordinal() + 1;
        }

    }

    public static class PlayBufferingFormatReducer extends
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
        job.setJarByClass(PlayBufferingFormatMR.class);
        job.setJobName("PlayBufferingFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/play_buffering"));
        FileOutputFormat.setOutputPath(job, new Path("output/play_buffering"));
        job.setMapperClass(PlayBufferingFormatMapper.class);
        job.setReducerClass(PlayBufferingFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
