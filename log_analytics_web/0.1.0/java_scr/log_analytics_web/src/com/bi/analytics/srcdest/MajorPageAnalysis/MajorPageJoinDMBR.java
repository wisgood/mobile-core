package com.bi.analytics.srcdest.MajorPageAnalysis;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

import com.bi.common.dm.pojo.fieldEnum.DMURLBounceCountEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;

/**
 * 
 * @ClassName: MajorPageJoinDMBR
 * @Description: 将跳出率(Bounce Rate)与按URL其他指标合并
 * @author liuyn
 * @date July 25, 2013 AM
 * 
 */

public class MajorPageJoinDMBR extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    public static class MajorPageJoinDMBRMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);

                // dmUrlBounceCount
                if (fields.length == DMURLBounceCountEnum.BOUNCE_COUNT
                        .ordinal() + 1) {
                    String urlbouncelinebycomma = line.replaceAll(SEPARATOR,
                            ",");

                    context.write(
                            new Text(fields[DMURLBounceCountEnum.URL.ordinal()]
                                    .trim().toLowerCase()), new Text(
                                    urlbouncelinebycomma));
                }

                // MajorPageOutBR
                else {
                    String urlStr = fields[MajorPageOutBREnum.URL.ordinal()];
                    context.write(new Text(urlStr.trim().toLowerCase()),
                            new Text(line));
                }
            }
            catch(ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }

        }
    }

    public static class MajorPageJoinDMBRReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String UrlBounceCountStr = null;
            List<String> majorPageInfoList = new ArrayList<String>();
            
            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(",")) {
                    UrlBounceCountStr = value;
                }
                else {
                    majorPageInfoList.add(value);
                }
            }
            
            for (String majorPageInfoStr : majorPageInfoList) {

                String majorPageETLValue = "";
                String[] majorPageInfoSts = majorPageInfoStr.split(SEPARATOR);

                List<String> splitLandPageList = new ArrayList<String>();
                for (String splitDownload : majorPageInfoSts) {
                    splitLandPageList.add(splitDownload);
                }

                if (null != UrlBounceCountStr) {
                    String[] urlBounceCountStrs = UrlBounceCountStr.split(",");

                    splitLandPageList.add(MajorPageOutBREnum.SESSION_COUNT
                            .ordinal(),
                            urlBounceCountStrs[DMURLBounceCountEnum.BOUNCE_COUNT
                                    .ordinal()]);

                }
                else {

                    splitLandPageList.add(
                            MajorPageOutBREnum.SESSION_COUNT.ordinal(), "0");

                }
                for (int i = 0; i < splitLandPageList.size(); i++) {

                    majorPageETLValue += splitLandPageList.get(i);
                    if (i < splitLandPageList.size()) {
                        majorPageETLValue += SEPARATOR;
                    }
                }

                context.write(new Text(majorPageETLValue), new Text(""));
            }

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

        nRet = ToolRunner.run(new Configuration(), new MajorPageJoinDMBR(),
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
        Job job = new Job(conf, "MajorPageJoinDMBR");
        job.setJarByClass(MajorPageJoinDMBR.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MajorPageJoinDMBRMapper.class);
        job.setReducerClass(MajorPageJoinDMBRReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(50);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
