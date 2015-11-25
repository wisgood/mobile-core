/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayerSEOFormat.java 
 * @Package com.bi.log.playerSEO.format.correctdata 
 * @Description: 对日志名进行处理
 * @author liuyn
 * @date Sep 1, 2013 11:53:02 PM 
 */
package com.bi.log.playerSEO.format.correctdata;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bi.common.dm.pojo.dao.AbstractDMDAO;
import com.bi.common.dm.pojo.dao.DMIPRuleDAOImpl;
import com.bi.common.init.ConstantEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.IPFormatUtil;
import com.bi.common.util.MACFormatUtil;
import com.bi.common.util.TimestampFormatUtil;
import com.bi.log.playerSEO.format.dataenum.PlayerSEOEnum;
import com.bi.log.pv.format.correctdata.PvFormatMR.PvFormatMap;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class PlayerSEOFormat extends Configured implements Tool {
    public static class PlayerSEOFormatMap extends
            Mapper<LongWritable, Text, NullWritable, Text> {

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        private static final String SEPARATOR = "\t";

        private static final String INPUTSEPARATOR = ",";
        
        private static final int MAX_VALUE = 65536;

        private static Logger logger = Logger.getLogger(PvFormatMap.class
                .getName());

        private static final String[] SHOWLOCARR = { "s", "0", "m", "1", "n", "2" };

        private static final String[] COOPTYPEARR = { "show", "0", "close", "1", "down", "2" };

        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));

        }

        public String getPlayerSEOFormatStr(String originalData) {
            StringBuilder exitETLSB = new StringBuilder();
            String[] splitSts = originalData.split(INPUTSEPARATOR);
            if (splitSts.length <= PlayerSEOEnum.QUDAO_ID.ordinal()) {
                return null;
            }

            try {

                long tStart = System.currentTimeMillis();
                String originalDataTranf = originalData.replaceAll(INPUTSEPARATOR, SEPARATOR);
                
                String timestampInfoStr = splitSts[PlayerSEOEnum.TIMESTAMP
                        .ordinal()];
                java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                        .formatTimestamp(timestampInfoStr);

                // dateId
                String dateId = formatTimesMap.get(ConstantEnum.DATE_ID);
                String hourIdStr = formatTimesMap.get(ConstantEnum.HOUR_ID);

                // hourid
                int hourId = Integer.parseInt(hourIdStr);

                String ipInfoStr = splitSts[PlayerSEOEnum.IP.ordinal()];
                long ipLong = 0;
                ipLong = IPFormatUtil.ip2long(ipInfoStr);
                java.util.Map<ConstantEnum, String> ipRuleMap = this.dmIPRuleDAO
                        .getDMOjb(ipLong);
                String provinceId = ipRuleMap.get(ConstantEnum.PROVINCE_ID);
                String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);

                String macInfoStr = splitSts[PlayerSEOEnum.MAC.ordinal()];
                MACFormatUtil.isCorrectMac(macInfoStr);
                String macInfor = MACFormatUtil
                        .macFormatToCorrectStr(macInfoStr);

                String coopTypeStr = splitSts[PlayerSEOEnum.COOPTYPE.ordinal()];
                int coopTypeId = MAX_VALUE;
                if (coopTypeStr != null && !"".equals(coopTypeStr)) {
                    for (int i = 0; i < COOPTYPEARR.length - 1; i = i + 2) {
                        if (COOPTYPEARR[i].equals(coopTypeStr)) {
                            coopTypeId = Integer.parseInt(COOPTYPEARR[i + 1]);
                            break;
                        }
                    }
                }

                String showLocStr = splitSts[PlayerSEOEnum.WHERE.ordinal()];
                int showLocId = MAX_VALUE;
                if (showLocStr != null && !"".equals(showLocStr)) {
                    for (int i = 0; i < SHOWLOCARR.length - 1; i = i + 2) {
                        if (SHOWLOCARR[i].equals(showLocStr)) {
                            showLocId = Integer.parseInt(SHOWLOCARR[i + 1]);
                            break;
                        }
                    }
                }

                if (coopTypeId == MAX_VALUE || showLocId == MAX_VALUE)
                    return null;

                exitETLSB.append(dateId + SEPARATOR);
                exitETLSB.append(hourId + SEPARATOR);
                exitETLSB.append(provinceId + SEPARATOR);
                exitETLSB.append(cityId + SEPARATOR);
                exitETLSB.append(macInfor + SEPARATOR);
                exitETLSB.append(coopTypeId + SEPARATOR);
                exitETLSB.append(showLocId + SEPARATOR);
                exitETLSB.append(originalDataTranf.trim());

            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                logger.error("error originalData:" + originalData);
                logger.error(e.getMessage(), e.getCause());
            }

            return exitETLSB.toString();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String PlayerSEOETLStr = this.getPlayerSEOFormatStr(line);
            if (null != PlayerSEOETLStr
                    && !("".equalsIgnoreCase(PlayerSEOETLStr))) {
                context.write(
                        NullWritable.get(), new Text(PlayerSEOETLStr));
            }
        }

    }

    /**
     * @param args
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

        nRet = ToolRunner.run(new Configuration(), new PlayerSEOFormat(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();

        Job job = new Job(conf, "PlayerSEOFormat");
        //job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setJarByClass(PlayerSEOFormat.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true); 
        job.setMapperClass(PlayerSEOFormatMap.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
