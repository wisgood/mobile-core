package com.bi.analytics.srcdest.PageTypeAnalysis;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
import com.bi.log.pv.format.dataenum.PvFormatEnum;

/**
 * 
 * @ClassName: PageTypeMetric
 * @Description: 页面类型详细指标分析，
 * @author liuyn
 * @date July 25, 2013 6:49:31 AM
 */

public class PageTypeMetric extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    // private static final String PLAY_PAGE_ID = "13";

    private static final String NEW_USER_FLAG_ID = "1";

    public static class PageTypeMetricMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split( SEPARATOR);
            String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
            String urlThirdIdStr = fields[PvFormatEnum.URL_THIRD_ID.ordinal()];
            String sessionStr = "";
            String fckStr = "";
            try {
                fckStr = fields[PvFormatEnum.FCK.ordinal()];
                sessionStr = fields[PvFormatEnum.SESSIONID.ordinal()];
            }
            catch(ArrayIndexOutOfBoundsException e) {
                return;
            }

            String userFlagIdStr = fields[PvFormatEnum.USER_FLAG.ordinal()];

            int urlThirdId = 0;
            int dateId = 0;

            try {
                if(urlThirdIdStr != null && dateIdStr != null){
                    urlThirdId = Integer.parseInt(urlThirdIdStr);
                    dateId = Integer.parseInt(dateIdStr);
                }
            }
            catch(NumberFormatException e) {
                return;
            }

            if (urlThirdId < 1000) {
                context.write(new Text(dateId + SEPARATOR + urlThirdId),
                        new Text(fckStr + SEPARATOR + userFlagIdStr + SEPARATOR
                                + sessionStr));
            }
        }
    }

    public static class PageTypeMetricReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            long PV = 0;

            Set<String> UVSet = new HashSet<String>();
            Set<String> newUVSet = new HashSet<String>();
            Set<String> sessionSet = new HashSet<String>();

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                if (fields.length < 3)
                    continue;
                String fckStr = fields[0];
                String userFlagIdStr = fields[1];
                String sessionStr = fields[2];

                PV++;
                UVSet.add(fckStr);
                sessionSet.add(sessionStr);

                if (NEW_USER_FLAG_ID.equals(userFlagIdStr)) {
                    newUVSet.add(fckStr);
                }
            }

            context.write(new Text(key.toString()), new Text(PV + SEPARATOR
                    + UVSet.size() + SEPARATOR + newUVSet.size() + SEPARATOR
                    + sessionSet.size()));
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
        ParamParse paramParse = new ParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new PageTypeMetric(),
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
        Job job = new Job(conf, "PageTypeMetric");
        job.setJarByClass(PageTypeMetric.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(PageTypeMetricMapper.class);
        job.setReducerClass(PageTypeMetricReducer.class);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(50);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
