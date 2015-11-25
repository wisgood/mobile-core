package com.bi.analytics.seo;

import java.io.IOException;
import java.util.HashSet;

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
 * 
 * @ClassName: SeoKeywordAnalyzer
 * @Description: 分搜索引擎关键词分析类
 * @author liuyn
 * @date July 25, 2013 6:49:31 AM
 */

public class SeoKeywordAnalyzer extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final int SEO_FLAG_ID = 1002;

    public static class SeoKWMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);

            String dateId = fields[PvFormatEnum.DATE_ID.ordinal()];

            String keywordStr = fields[PvFormatEnum.OUTER_SEARCH_KEYWORD
                    .ordinal()];
            String fckStr = fields[PvFormatEnum.FCK.ordinal()];
            String urlFirstIdStr = fields[PvFormatEnum.URL_FIRST_ID.ordinal()];
            String referSecondIdStr = fields[PvFormatEnum.REFER_SECOND_ID
                    .ordinal()];
            String referFirstIdStr = fields[PvFormatEnum.REFRE_FIRST_ID.ordinal()];

            int referFirstId = 0;
            int referSecondId = 0;
            try {
                referFirstId = Integer.parseInt(referFirstIdStr);
                referSecondId = Integer.parseInt(referSecondIdStr);
            }
            catch(NumberFormatException e) {
                return;
            }

            if ("1".equals(urlFirstIdStr) && SEO_FLAG_ID == referFirstId ) {
                context.write(new Text(dateId + SEPARATOR + referSecondId
                        + SEPARATOR + keywordStr), new Text(fckStr));
            }

        }

    }

    public static class SeoKWReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            HashSet<String> UVSet = new HashSet<String>();
            int pv = 0;
            for (Text value : values) {
                String fckStr = value.toString();
                UVSet.add(fckStr);
                pv++;
            }

            context.write(new Text(key.toString()), new Text(pv
                    + SEPARATOR + UVSet.size()));

        }

    }

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

        nRet = ToolRunner.run(new Configuration(), new SeoKeywordAnalyzer(),
                paramParse.getParams());
        System.out.println(nRet);

    }



    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "SeoKeywordAnalyzer");
        job.setJarByClass(SeoKeywordAnalyzer.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SeoKWMapper.class);
        job.setReducerClass(SeoKWReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
