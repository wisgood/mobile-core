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

import com.bi.client.quality.enums.DtfspEnum;
import com.bi.client.quality.enums.PlayHaltEnum;
import com.bi.client.quality.enums.PlayHaltFormatEnum;
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
public class DtfspFormatMR {

    public static class DtfspFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(DtfspFormatMapper.class.getName());

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            logger.info("DtfspETLMap rummode is cluster!");
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";

            try {

                String timeStampOrigin = fields[DtfspEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[DtfspEnum.VV.ordinal()];
                String macOrigin = fields[DtfspEnum.MAC.ordinal()];
                String clientIPOrigin = fields[DtfspEnum.CLIENTIP.ordinal()];
                String leOrigin = fields[DtfspEnum.LE.ordinal()];
                String tuOrigin = fields[DtfspEnum.SPT.ordinal()];
//                String infohashOrigin = fields[DtfspEnum.IH.ordinal()];

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

                String leFormat = validateFileStatus(leOrigin);
                String tuFormat = validateNumber(tuOrigin);
                

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
                formatLog.append(leFormat);
                formatLog.append("\t");
                formatLog.append(tuFormat);
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
        * @Title: validateFileStatus 
        * @Description: 验证下载文件状态,只能是０,1,2,3，若有其他则置为-1 
        * @param   @param fileStatusOrigin
        * @param   @return 参数说明
        * @return String    返回类型说明 
        * @throws
         */
        private String validateFileStatus(String leOrigin) {
            String result = "-1";
            if(leOrigin.compareToIgnoreCase("0")==0 ||
                    leOrigin.compareToIgnoreCase("1")==0||
                    leOrigin.compareToIgnoreCase("2")==0||
                    leOrigin.compareToIgnoreCase("3")==0){
                result = leOrigin;
            }
            return result;
        }
        
        /**
         * @ClassName: validateNumber
         * @Description: 验证是否为是否为大于等于０的整数，若不是置为-1
         * @author niewf
         * @date Aug 29, 2013 8:40:53 PM
         */
        /**
        *
        * @Title: validateNumber 
        * @Description: 这里用一句话描述这个方法的作用
        * @Auther: niewf
        * @Date: Sep 2, 2013
         */
        private String validateNumber(String numOrigin) {
            String result = "-1";
            long num = 0;
            try{
                num = Long.parseLong(numOrigin);
                if(num >= 0){
                    result = numOrigin;
                }
            }catch (Exception e){
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
                                formatLog.split("\t")[PlayHaltFormatEnum.TIMESTAMP
                                        .ordinal()]), new Text(formatLog));
            }
        }

        private boolean lengthMeet(int length) {

            return length >= PlayHaltEnum.ISP.ordinal() + 1;
        }

    }

    public static class DtfspFormatReducer extends
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
        job.setJarByClass(DtfspFormatMR.class);
        job.setJobName("PlayBufferingFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/dtfsp"));
        FileOutputFormat.setOutputPath(job, new Path("output/dtfsp"));
        job.setMapperClass(DtfspFormatMapper.class);
        job.setReducerClass(DtfspFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
