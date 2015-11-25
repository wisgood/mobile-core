package com.bi.analytics.srcdest.LandPageAnalysis;

import jargs.gnu.CmdLineParser.Option;

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
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class LandPageSrc extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final int EXTER_SRC_PAGETYPE = 1;

    public static class LandPageSrcMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(SEPARATOR);
            
            String dateIdStr  = fields[PvFormatEnum.DATE_ID.ordinal()];

            String fckStr = fields[PvFormatEnum.FCK.ordinal()];

            String referStr = fields[PvFormatEnum.REFERURL.ordinal()];

            String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID.ordinal()];

            String urlFirstIdStr = fields[PvFormatEnum.URL_FIRST_ID.ordinal()];
            
            if (referStr == null || "".equals(referStr))
                return ;
            String referLastStr = StringDecodeFormatUtil.urlNormalizer(referStr);
            if (referLastStr  == null || "".equals(referLastStr))
            {
                return;
            }
            

            int referThirdId = 0;
            try {
                referThirdId = Integer.parseInt(referThirdIdStr);
            }
            catch(NumberFormatException e) {
                return;
            }
            
           
                
            if (referThirdId > 1000 && "1".equals(urlFirstIdStr)) {                
                context.write(new Text(dateIdStr + SEPARATOR
                        + EXTER_SRC_PAGETYPE + SEPARATOR
                        + referLastStr),
                        new Text(fckStr));
            }
        }

    }

    public static class LandPageSrcReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            long PV = 0;
            Set<String> UVSet = new HashSet<String>();

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                String fckStr = fields[0];
                PV++;
                UVSet.add(fckStr);
            }
            context.write(new Text(key.toString()), new Text(PV + SEPARATOR
                    + UVSet.size()));
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

        nRet = ToolRunner.run(new Configuration(), new LandPageSrc(),
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
        Job job = new Job(conf, "LandPageSrc");
        job.setJarByClass(LandPageSrc.class);
        for (String path : args[0].split("\t")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(LandPageSrcMapper.class);
        job.setReducerClass(LandPageSrcReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(30);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
