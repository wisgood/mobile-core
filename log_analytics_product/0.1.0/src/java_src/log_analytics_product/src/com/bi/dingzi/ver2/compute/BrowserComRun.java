package com.bi.dingzi.ver2.compute;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: dd.java 
 * @Package com.bi.dingzi.ver2.compute 
 * @Description: 浏览器组件指标程序计算
 * @author wangxiaowei
 * @date 2013-7-25 下午10:50:51 
 * @input:输入日志路径: /dw/logs/tools/result/ver2/BrowserComRun/$DIR_DAY
 * @output:输出日志路径: /dw/logs/tools/result/ver2/liulanqi/$DIR_DAY
 * @executeCmd:hadoop jar ....
 * @inputFormat:DATEID HOURID IP  CATEGORY  NAME  VERSION  MAC  GUID  BRONAME  BROVERSION  SUC  URL  TYPE  STRATERY  VERSIONID
 * @ouputFormat:DateId BCC_ID VER_ID DZ_QD_S_USER_NUM DZ_QD_S_NUM DZ_QD_USER_NUM  DZ_QD_NUM  DZ_LA_USER_NUM  DZ_LA_NUM  DZ_LAQIC_S_USER_NUM  DZ_LAQIC_S_NUM  DZ_LAQI_USER_NUM  DZ_LAQI_NUM  DZ_LAQID_S_USER_NUM  DZ_LAQID_S_NUM 
 */
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.DateFormat;
import com.bi.common.util.DateFormatInfo;

public class BrowserComRun extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final int LENGTH = 15;

    private enum BrowserComRumFormatEnum {
        DATEID, HOURID, IP, CATEGORY, NAME, VERSION, MAC, GUID, BRONAME, BROVERSION, SUC, URL, TYPE, STRATERY, VERSIONID;
    }

    public static class BrowserComRunMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String valueStr = value.toString();
            String[] fields =  DateFormat.split(valueStr, DateFormatInfo.SEPARATOR, 0);
            if (fields.length < LENGTH)
                return;
            String date = fields[BrowserComRumFormatEnum.DATEID.ordinal()];
            String outKey = date + SEPARATOR
                    + fields[BrowserComRumFormatEnum.CATEGORY.ordinal()]
                    + SEPARATOR
                    + fields[BrowserComRumFormatEnum.VERSIONID.ordinal()];
            String success = fields[BrowserComRumFormatEnum.SUC.ordinal()];
            String type = fields[BrowserComRumFormatEnum.TYPE.ordinal()];
            String strategy = fields[BrowserComRumFormatEnum.STRATERY.ordinal()];
            String mac = fields[BrowserComRumFormatEnum.MAC.ordinal()];
            String outValue = mac + SEPARATOR + success + SEPARATOR + type
                    + SEPARATOR + strategy;
            context.write(new Text(outKey), new Text(outValue));

        }

    }

    public static class BrowserComRunReducer extends
            Reducer<Text, Text, Text, Text> {
        private long bootSuccessUser;

        private long bootSuccessRecords;

        private long bootUser;

        private long bootRecords;

        private long pullClientSuccessUser;

        private long pullClientSuccessRecords;

        private long pullClientUser;

        private long pullClientRecords;

        private long pullDingziSuccessUser;

        private long pullDingziSuccessRecords;

        private long pullDingziUser;

        private long pullDingziRecords;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> bootSuccessUserMac = new HashSet<String>();
            Set<String> bootUserMac = new HashSet<String>();
            Set<String> pullClientSuccessUserMac = new HashSet<String>();
            Set<String> pullClientUserMac = new HashSet<String>();
            Set<String> pullDingziSuccessUserMac = new HashSet<String>();
            Set<String> pullDingziUserMac = new HashSet<String>();

            for (Text value : values) {
                String[] fields = value.toString().replace('\t', ',')
                        .split(",");
                String mac = fields[0];
                int successTag = -1;
                int typeTag = -1;
                int strategyTag = -1;
                try {
                    successTag = Integer.parseInt(fields[1]);
                    typeTag = Integer.parseInt(fields[2]);
                    strategyTag = Integer.parseInt(fields[3]);
                }
                catch(Exception e) {
                }
                // process success
                if (1 == successTag) {
                    bootSuccessUserMac.add(mac);
                    bootSuccessRecords++;
                    bootUserMac.add(mac);
                    bootRecords++;
                }
                else if (0 == successTag) {
                    bootUserMac.add(mac);
                    bootRecords++;
                }
                // process pull client
                if (0 == typeTag) {
                    pullClientRecords++;
                    pullClientUserMac.add(mac);
                    if (strategyTag == 1 && successTag == 2) {
                        pullClientSuccessRecords++;
                        pullClientSuccessUserMac.add(mac);
                    }
                }

                // process pull dingzi
                if (1 == typeTag) {

                    pullDingziUserMac.add(mac);
                    pullDingziRecords++;
                    if (strategyTag == 1 && successTag == 2) {
                        pullDingziSuccessUserMac.add(mac);
                        pullDingziSuccessRecords++;
                    }
                }

            }
            bootSuccessUser = bootSuccessUserMac.size();
            bootUser = bootUserMac.size();
            pullClientSuccessUser = pullClientSuccessUserMac.size();
            pullClientUser = pullClientUserMac.size();
            pullDingziUser = pullDingziUserMac.size();
            pullDingziSuccessUser = pullDingziSuccessUserMac.size();

            StringBuilder outputKey = new StringBuilder();
            outputKey.append(bootSuccessUser + SEPARATOR);
            outputKey.append(bootSuccessRecords + SEPARATOR);
            outputKey.append(bootUser + SEPARATOR);
            outputKey.append(bootRecords + SEPARATOR);
            outputKey.append(pullClientUser + SEPARATOR);
            outputKey.append(pullClientRecords + SEPARATOR);
            outputKey.append(pullClientSuccessUser + SEPARATOR);
            outputKey.append(pullClientSuccessRecords + SEPARATOR);
            outputKey.append(pullDingziUser + SEPARATOR);
            outputKey.append(pullDingziRecords + SEPARATOR);
            outputKey.append(pullDingziSuccessUser + SEPARATOR);
            outputKey.append(pullDingziSuccessRecords);

            context.write(key, new Text(outputKey.toString()));

        }
    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new BrowserComRun(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "dingzi_browsercom");
        job.setJarByClass(BrowserComRun.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BrowserComRunMapper.class);
        job.setReducerClass(BrowserComRunReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
