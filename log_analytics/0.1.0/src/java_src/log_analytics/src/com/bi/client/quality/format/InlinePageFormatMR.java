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

import com.bi.client.quality.enums.InlinePageEnum;
import com.bi.client.quality.enums.InlinePageFormatEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * 
* @ClassName: InlinePageFormatMR 
* @Description: 这里用一句话描述这个类的作用 
* @author niewf
* @date Sep 2, 2013 7:42:40 PM
 */

public class InlinePageFormatMR {

    public static class InlinePageFormatMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(InlinePageFormatMapper.class.getName());

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();

            logger.info("InlinePageETLMap rummode is cluster!");
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        public String logFormat(String originLog) {
            String[] fields = originLog.split("\t");
            final String empty = "";

            try {

                String timeStampOrigin = fields[InlinePageEnum.TIMESTAMP
                        .ordinal()];
                String versionOrigin = fields[InlinePageEnum.VV.ordinal()];
                String macOrigin = fields[InlinePageEnum.MAC.ordinal()];
                String clientIPOrigin = fields[InlinePageEnum.CLIENTIP.ordinal()];
                String tuOrigin = fields[InlinePageEnum.TU.ordinal()];
                String werrOrigin = fields[InlinePageEnum.WERR.ordinal()];

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
                String tuFormat = validNumber(tuOrigin);
                String werrFormat = validErrorCode(werrOrigin, tuOrigin);
                

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
                formatLog.append(tuFormat);
                formatLog.append("\t");
                formatLog.append(werrFormat);
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
         * @ClassName: validNumber
         * @Description: 验证是否为是否为整数，若不是置为-1
         * @author niewf
         * @date Aug 29, 2013 8:40:53 PM
         */
        private String validNumber(String numOrigin) {
            String result = numOrigin;
            try{
                long num = Long.parseLong(numOrigin);
            }catch (Exception e){
                result = "-1";
                System.out.println("This is not a Number, " + numOrigin);
            }
            return result;
        }

        /**
         * @Title: validErrorCode
         * @Description: 验证打开错误代码
         * @Auther: niewf
         * @Date: Sep 2, 2013 8:16:29 PM
         */
        private String validErrorCode(String errorCodeOrigin,
                String timeUsedOrigin) {
            String result = errorCodeOrigin;

            if (errorCodeOrigin.compareToIgnoreCase("undef") == 0) {
                // 错误代码为undef时,通过检查打开时间补齐
                long timeUsed = 0;
                try {
                    timeUsed = Long.parseLong(timeUsedOrigin);
                    if (timeUsed > 0) { //　成功,错误代码用０表示
                        result = "0";
                    }
                    else { // 失败,错误代码为时间去除减号
                        result = timeUsedOrigin.substring(1);
                    }
                }
                catch(Exception e){
                    result = "-1";
                    System.out.println("This is not a Number, "
                            + timeUsedOrigin);
                }
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
                                formatLog.split("\t")[InlinePageFormatEnum.TIMESTAMP
                                        .ordinal()]), new Text(formatLog));
            }
        }

        private boolean lengthMeet(int length) {

            return length >= InlinePageEnum.ISP.ordinal() + 1;
        }

    }

    public static class InlinePageFormatReducer extends
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
        job.setJarByClass(InlinePageFormatMR.class);
        job.setJobName("PlayHaltFormat");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.addInputPath(job, new Path("input/inline_page"));
        FileOutputFormat.setOutputPath(job, new Path("output/inline_page"));
        job.setMapperClass(InlinePageFormatMapper.class);
        job.setReducerClass(InlinePageFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
