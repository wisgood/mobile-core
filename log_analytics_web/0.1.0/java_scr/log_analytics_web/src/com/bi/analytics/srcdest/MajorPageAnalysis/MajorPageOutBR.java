package com.bi.analytics.srcdest.MajorPageAnalysis;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.dm.pojo.fieldEnum.DMMajorPageUrlEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

/**
 * 
 * @ClassName: MajorPageOutBR
 * @Description: 计算重要页面相关指标，如PV, UV, NewUV, SessionCount等,但不包括跳出率(Bounce Rate)
 * @author liuyn
 * @date July 25, 2013 7:51:23 AM
 */

public class MajorPageOutBR extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final String NEW_USER_FLAG_ID = "1";

    public static class MajorPageOutBRMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);

                /*
                 * FileSplit fileSplit = (FileSplit) context.getInputSplit();
                 * 
                 * String fileName = fileSplit.getPath().toString();
                 * System.out.println(fileName);
                 */

                // MajorPage
                if (fields.length == DMMajorPageUrlEnum.URL_DESC.ordinal() + 1) {
                    String DmMajorPagebycomma = line.replaceAll(SEPARATOR, ",");
                    String urlStr = StringDecodeFormatUtil
                            .urlNormalizer(fields[DMMajorPageUrlEnum.URL
                                    .ordinal()]);
                    context.write(new Text(urlStr),
                            new Text(DmMajorPagebycomma));
                }

                // Pv origin
                else {
                    String urlStr = StringDecodeFormatUtil
                            .urlNormalizer(fields[PvFormatEnum.URL.ordinal()]);
                    String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
                    String fckStr = fields[PvFormatEnum.FCK.ordinal()];
                    String userFlagIdStr = fields[PvFormatEnum.USER_FLAG
                            .ordinal()];
                    String sessionIdStr = fields[PvFormatEnum.SESSIONID
                            .ordinal()];

                    context.write(new Text(urlStr), new Text(fckStr + SEPARATOR
                            + userFlagIdStr + SEPARATOR + sessionIdStr
                            + SEPARATOR + dateIdStr));
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static class MajorPageOutBRReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String DmMajorPageStr = "";
            List<String> pvInfoList = new ArrayList<String>();

            for (Text val : values) {
                String value = val.toString().trim();
                if (value.contains(",")) {
                    DmMajorPageStr = value;
                }
                else {
                    pvInfoList.add(value);
                }
            }
            long PV = 0;
            String dateIdStr = "";
            Set<String> UVSet = new HashSet<String>();
            Set<String> newUVSet = new HashSet<String>();
            Set<String> sessionSet = new HashSet<String>();

            if (null != DmMajorPageStr && !"".equals(DmMajorPageStr)) {

                for (String pvInfoStr : pvInfoList) {
                    String[] fields = pvInfoStr.split(SEPARATOR);
                    String fckStr = fields[0];
                    String userFlagIdStr = fields[1];
                    String sessionIdStr = fields[2];
                    dateIdStr = fields[3];

                    PV++;
                    UVSet.add(fckStr);
                    if (NEW_USER_FLAG_ID.equals(userFlagIdStr)) {
                        newUVSet.add(fckStr);
                    }
                    sessionSet.add(sessionIdStr);
                }
                
                context.write(new Text(dateIdStr + SEPARATOR +  key.toString()), new Text(PV + SEPARATOR
                        + UVSet.size() + SEPARATOR + newUVSet.size()
                        + SEPARATOR + sessionSet.size()));
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

        nRet = ToolRunner.run(new Configuration(), new MajorPageOutBR(),
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
        Job job = new Job(conf, "MajorPageOutBR");
        job.setJarByClass(MajorPageOutBR.class);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MajorPageOutBRMapper.class);
        job.setReducerClass(MajorPageOutBRReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(40);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
