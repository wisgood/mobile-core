/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayerRemindAnalyzer.java 
 * @Package com.bi.analytics.seo 
 * @Description: 对日志名进行处理
 * @author liuyn
 * @hdfs playerSEO output
 * @date Sep 2, 2013 11:02:26 PM 
 */
package com.bi.analytics.seo;

import java.io.IOException;
import java.util.HashMap;
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
import com.bi.log.playerSEO.format.dataenum.PlayerSEOFormatEnum;

/**
 * @ClassName: PlayerRemindAnalyzer
 * @Description: 播放器安装提醒框分析类
 * @author liuyn
 * @date Sep 2, 2013 11:02:26 PM
 */
public class PlayerSEOAnalyzer extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final int coopTypeSum = 3;

    public static class PlayerSEOAnaMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(SEPARATOR);

            String dateIdStr = fields[PlayerSEOFormatEnum.DATE_ID.ordinal()];
            String quDaoIdStr = fields[PlayerSEOFormatEnum.QUDAO_ID.ordinal()];
            String showLocIdStr = fields[PlayerSEOFormatEnum.SHOWLOC_ID
                    .ordinal()];
            String coopTypeIdStr = fields[PlayerSEOFormatEnum.COOPTYPE_ID
                    .ordinal()];
            String fckStr = fields[PlayerSEOFormatEnum.FCK.ordinal()];
            int quDaoId = 0;
            try {
                if (!"".equals(quDaoIdStr) && quDaoIdStr != null)
                    quDaoId = Integer.parseInt(quDaoIdStr);
            }
            catch(NumberFormatException e) {
            }

            context.write(new Text(dateIdStr + SEPARATOR + quDaoId + SEPARATOR
                    + showLocIdStr), new Text(coopTypeIdStr + SEPARATOR
                    + fckStr));

        }
    }

    public static class PlayerSEOAnaReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int[] pvArr = new int[coopTypeSum];
            HashMap<Integer, Set<String>> UVMap = new HashMap<Integer, Set<String>>(coopTypeSum);
            StringBuilder indicatorSB = new StringBuilder();
            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                if (fields.length < 2)
                    continue;
                int coopTypeId = 0;
                try {
                    coopTypeId = Integer.parseInt(fields[0]);
                }
                catch(NumberFormatException e) {
                    continue;
                }

                String fckStr = fields[1];
                pvArr[coopTypeId]++;
                if (UVMap.containsKey(coopTypeId))
                    UVMap.get(coopTypeId).add(fckStr);
                else
                    UVMap.put(coopTypeId, new HashSet<String>());
            }

            for (int i = 0; i < coopTypeSum; i++) {
                indicatorSB.append(pvArr[i] + SEPARATOR);
                if (UVMap.containsKey(i))
                    indicatorSB.append(UVMap.get(i).size());
                else
                    indicatorSB.append(0);
                if(i < coopTypeSum -1)
                    indicatorSB.append(SEPARATOR);

            }

            context.write(new Text(key), new Text(indicatorSB.toString()));

        }

    }

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

        nRet = ToolRunner.run(new Configuration(), new PlayerSEOAnalyzer(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();

        Job job = new Job(conf, "PlayerSEOAnalyzer");
        job.setJarByClass(PlayerSEOAnalyzer.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        // job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        job.setMapperClass(PlayerSEOAnaMapper.class);
        job.setReducerClass(PlayerSEOAnaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
