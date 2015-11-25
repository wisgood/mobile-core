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
import com.bi.log.pv.format.dataenum.PvFormatEnum;

/**
 * @ClassName: QuDaoAnalyzer
 * @Description: 渠道推广分析，包括页面导入效果分析
 * @author liuyn
 * @date Sep 2, 2013 11:02:26 PM
 */

public class QuDaoAnalyzer extends Configured implements Tool {

    private static final String SEPARATOR = "\t";
    
    private static final int[] PAGETYPEIDARR = {1,11,12,13,17};

    public static class QuDaoAnalyzerMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);

            String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
            String fckStr = fields[PvFormatEnum.FCK.ordinal()];
            String ipStr = fields[PvFormatEnum.IP.ordinal()];
            String macCodeStr = fields[PvFormatEnum.MAC_CODE.ordinal()];
            String urlFirstIdStr = fields[PvFormatEnum.URL_FIRST_ID.ordinal()];
            String urlSecondIdStr = fields[PvFormatEnum.URL_SECOND_ID.ordinal()];
            String quDaoIdStr = fields[PvFormatEnum.ISPID.ordinal()];
            String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID.ordinal()];
            
            int quDaoId = 0;
            int referThirdId = 0;
            try {
                if(!"".equals(quDaoIdStr) && quDaoIdStr != null){
                    quDaoId = Integer.parseInt(quDaoIdStr);
                    referThirdId = Integer.parseInt(referThirdIdStr);
                }
            }
            catch(NumberFormatException e) {
                return;
            }
            
            if("1".equals(urlFirstIdStr) && referThirdId > 1000){
                context.write(new Text(dateIdStr + SEPARATOR + quDaoId),
                    new Text(fckStr  + SEPARATOR + ipStr
                            + SEPARATOR + macCodeStr + SEPARATOR + urlSecondIdStr));
            }

        }

    }

    public static class QuDaoAnalyzerReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            Set<String> UVSet = new HashSet<String>();
            Set<String> macSet = new HashSet<String>();
            Set<String> ipSet = new HashSet<String>();
            
            int []pvArr = new int[PAGETYPEIDARR.length];
            HashMap<Integer,Set<String>> UVMap = new HashMap<Integer,Set<String>>(PAGETYPEIDARR.length);
            HashMap<Integer,Set<String>> macMap = new HashMap<Integer,Set<String>>(PAGETYPEIDARR.length);            
            long pv = 0;
            StringBuilder indicatorSB = new StringBuilder();
            
            for (Text value : values) {
               String []fields = value.toString().split(SEPARATOR);
               String fckStr = fields[0];
               String ipStr = fields[1];
               String macCodeStr = fields[2];
               int pageTypeId = 0;
               try{
                   pageTypeId = Integer.parseInt(fields[3]);
               }
               catch(NumberFormatException e){
                   continue;
               }
              
               for(int i = 0; i< PAGETYPEIDARR.length; i++){
                   if(PAGETYPEIDARR[i] == pageTypeId){ 
                       if(UVMap.containsKey(i))
                           UVMap.get(i).add(fckStr);
                       else
                           UVMap.put(i, new HashSet<String>());
                       if(macMap.containsKey(i))
                           macMap.get(i).add(macCodeStr);
                       else
                           macMap.put(i, new HashSet<String>());
                       pvArr[i]++;
                       break;
                   }
               }
               
               pv++;
               UVSet.add(fckStr);
               macSet.add(macCodeStr);
               ipSet.add(ipStr);
               
            }
            
            for(int i = 0; i< PAGETYPEIDARR.length; i++){
                indicatorSB.append(SEPARATOR + pvArr[i]);
                if(UVMap.containsKey(i))
                    indicatorSB.append(SEPARATOR + UVMap.get(i).size());
                else
                    indicatorSB.append(SEPARATOR + 0);
                if(macMap.containsKey(i))
                    indicatorSB.append(SEPARATOR + macMap.get(i).size());
                else
                    indicatorSB.append(SEPARATOR + 0);
            }
            
            context.write(new Text(key),
                    new Text(pv  + SEPARATOR + UVSet.size() + SEPARATOR + macSet.size()
                            + SEPARATOR + ipSet.size() + indicatorSB.toString()));
        }

    }

    /**
     * @param args
     */
    /**
     * @param args
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

        nRet = ToolRunner.run(new Configuration(), new QuDaoAnalyzer(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "QuDaoAnalyzer");
        job.setJarByClass(QuDaoAnalyzer.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        
        job.setMapperClass(QuDaoAnalyzerMapper.class);
        job.setReducerClass(QuDaoAnalyzerReducer.class);
 
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(40);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
