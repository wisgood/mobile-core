package com.bi.analytics.srcdest.SummaryAnalysis;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: LandPageMR.java 
 * @Description: 
 * @author liuyn
 * @date 2013-08-15
 */

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.TreeMap;

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
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class LandPage extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    public static class LandPageMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orginData = value.toString();
            String[] fields = orginData.split(SEPARATOR);

            if (fields.length < PvFormatEnum.REFERURL.ordinal())
                return;
            String urlStr = fields[PvFormatEnum.URL.ordinal()];
            String urlResStr = StringDecodeFormatUtil.urlNormalizer(urlStr);

            String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
            String timestampStr = fields[PvFormatEnum.TIMESTAMP.ordinal()]
                    .trim();
            String sessionidStr = fields[PvFormatEnum.SESSIONID.ordinal()]
                    .trim();
            String urlFirstIdStr = fields[PvFormatEnum.URL_FIRST_ID.ordinal()];

            String fckStr = fields[PvFormatEnum.FCK.ordinal()];
            String userFlagIdStr = fields[PvFormatEnum.USER_FLAG.ordinal()];
            String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID
                    .ordinal()];

            if (!"".equalsIgnoreCase(urlResStr) && "1".equals(urlFirstIdStr)) {
                context.write(new Text(dateIdStr + SEPARATOR + sessionidStr),
                        new Text(timestampStr + SEPARATOR + urlResStr
                                + SEPARATOR + fckStr + SEPARATOR
                                + userFlagIdStr + SEPARATOR + referThirdIdStr));

            }
        }

    }

    public static class LandPageReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Long, String> timeUrlsTree = new TreeMap<Long, String>();
            
            StringBuilder pageInfo = new StringBuilder();
            
            for (Text value : values) {

                String valueStr = value.toString();
                String[] splists = valueStr.split(SEPARATOR);
                Long timeStamp = new Long(splists[0]);
                
                pageInfo.delete(0, pageInfo.length());

                try {
                    pageInfo.append(splists[1] + SEPARATOR);
                    pageInfo.append(splists[2] + SEPARATOR);
                    pageInfo.append(splists[3] + SEPARATOR);
                    pageInfo.append(splists[4]);
                }
                catch(Exception e) {
                    continue;
                }
                String pageInfoStr = pageInfo.toString();
                timeUrlsTree.put(timeStamp, pageInfoStr);

            }

            Long firstTimeStamp = timeUrlsTree.firstKey();
            String landPageInfoStr = timeUrlsTree.get(firstTimeStamp);
            context.write(key, new Text(landPageInfoStr));
        }

    }

    /**
     * @param args
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new LandPage(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    static class ParamParse extends AbstractCmdParamParse {

        @Override
        public String getFunctionDescription() {
            return "";
        }

        @Override
        public String getFunctionUsage() {
            return "";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            return new Option[0];
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "LandPage");
        job.setJarByClass(LandPage.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(LandPageMapper.class);
        job.setReducerClass(LandPageReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(40);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
