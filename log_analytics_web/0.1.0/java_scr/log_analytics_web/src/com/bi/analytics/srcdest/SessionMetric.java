package com.bi.analytics.srcdest;

import jargs.gnu.CmdLineParser.Option;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
import com.bi.log.pv.format.dataenum.PvFormatEnum;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * 
 * @ClassName: SessionMetric
 * @Description: 按照session确定站外来源(referFirstId)及后续VV
 * @author liuyn
 * @date July 25, 2013 6:49:31 AM
 */

public class SessionMetric extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final String PLAY_PAGE_STR = "13";

    public static class SessionMetricMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);

            String dateId = fields[PvFormatEnum.DATE_ID.ordinal()];
            String sessionStr = fields[PvFormatEnum.SESSIONID.ordinal()];
            String urlSecondIdStr = fields[PvFormatEnum.URL_SECOND_ID.ordinal()];
            String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID
                    .ordinal()];
            String urlFirstIdStr = fields[PvFormatEnum.URL_FIRST_ID.ordinal()];
            String timestampStr = fields[PvFormatEnum.TIMESTAMP.ordinal()]
                    .trim();

            int referThirdId = 0;
            try {
                referThirdId = Integer.parseInt(referThirdIdStr);
            }
            catch(NumberFormatException e) {
                return;
            }

            if ("1".equals(urlFirstIdStr)) {
                context.write(new Text(dateId + SEPARATOR + sessionStr),
                        new Text(urlSecondIdStr + SEPARATOR + referThirdId + SEPARATOR + timestampStr));
            }

        }

    }

    public static class SessionMetricReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int extReferId = 0;
            int vv = 0;
            TreeMap<Long, Integer> timeUrlsTree = new TreeMap<Long, Integer>();
            
            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                String urlSecondIdStr = fields[0];
                String referFirstIdStr = fields[1];
                int referFirstId = Integer.parseInt(referFirstIdStr);
                
                Long timeStamp = new Long(fields[2]);
                
                if (PLAY_PAGE_STR.equals(urlSecondIdStr)) {
                    vv++;
                }
                timeUrlsTree.put(timeStamp, referFirstId);
            }
            
            Long firstTimeStamp = timeUrlsTree.firstKey();
            extReferId = timeUrlsTree.get(firstTimeStamp);
            
            if (extReferId > 1000) {
                context.write(new Text(key.toString()), new Text(extReferId
                        + SEPARATOR + vv));
            }
        }

    }

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

        nRet = ToolRunner.run(new Configuration(), new SessionMetric(),
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
        Job job = new Job(conf, "SessionMetric");
        job.setJarByClass(SessionMetric.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SessionMetricMapper.class);
        job.setReducerClass(SessionMetricReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(50);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
