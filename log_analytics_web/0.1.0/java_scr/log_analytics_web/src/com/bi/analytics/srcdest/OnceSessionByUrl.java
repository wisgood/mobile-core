package com.bi.analytics.srcdest;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;

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

public class OnceSessionByUrl extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    public static class OnceSessionByUrlMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);

            String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
            String sessionIdStr = fields[PvFormatEnum.SESSIONID.ordinal()];
            String urlStr = fields[PvFormatEnum.URL.ordinal()];

            String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID
                    .ordinal()];
            int referThirdId = 0;
            try {
                referThirdId = Integer.parseInt(referThirdIdStr);
            }
            catch(NumberFormatException e) {
                return;
            }
            
            context.write(new Text(dateIdStr + SEPARATOR + sessionIdStr),
                    new Text(StringDecodeFormatUtil.urlNormalizer(urlStr) + SEPARATOR + referThirdId));

        }

    }

    public static class OnceSessionByUrlReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            
            String[] keyFields = key.toString().split(SEPARATOR);
            if(keyFields.length < 2) 
                return;
            String dateIdStr = keyFields[0];
            String sessionIdStr = keyFields[1];
            int counter = 1;

            String originValueStr = null;
            for (Text value : values) {
                if (counter == 2)
                    return;
                counter++;
                String[] fields = value.toString().split(SEPARATOR);
                if(fields.length < 2)
                    continue;
                originValueStr =fields[0];
                if(Integer.parseInt(fields[1]) < 1000)
                    return;
            }

            context.write(new Text(dateIdStr + SEPARATOR + originValueStr),
                    new Text(sessionIdStr));
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

        nRet = ToolRunner.run(new Configuration(), new OnceSessionByUrl(),
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
        Job job = new Job(conf, "OnceSessionByUrl");
        job.setJarByClass(OnceSessionByUrl.class);
        for (String path : args[0].split("\t")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(OnceSessionByUrlMapper.class);
        job.setReducerClass(OnceSessionByUrlReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
