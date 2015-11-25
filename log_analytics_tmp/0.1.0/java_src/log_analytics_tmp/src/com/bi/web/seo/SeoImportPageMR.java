/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: SeoImportPageMR.java 
 * @Package com.bi.web.seo 
 * @Description: 对日志名进行处理
 * @author fuys
 * @date 2013-11-7 下午3:43:23 
 * @input:输入日志路径/2013-11-7
 * @output:输出日志路径/2013-11-7
 * @executeCmd:hadoop jar ....
 * @inputFormat:DateId HourId ...
 * @ouputFormat:DateId MacCode ..
 */
package com.bi.web.seo;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.util.HdfsUtil;

/**
 * @ClassName: SeoImportPageMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-11-7 下午3:43:23
 */
public class SeoImportPageMR extends Configured implements Tool {
    private static final String LANDPAGE = "f_web_sd_landpage_metric";

    public static String decodeCodedStr(String sourceStr, String targetCharset)
            throws UnsupportedEncodingException {
        String decodedStr;
        String changedStr = sourceStr; // String changedStr =
                                       // changeCharset(sourceStr,
                                       // targetCharset);
        if (changedStr != null) {
            try {
                decodedStr = URLDecoder.decode(
                        URLDecoder.decode(changedStr, targetCharset),
                        targetCharset);
                // decodedStr = URLDecoder.decode(changedStr, targetCharset);
            }
            catch(Exception e) {
                decodedStr = "";
                return decodedStr;
            }
            return decodedStr;
        }
        return null;
    }

    public static class SeoImportPageMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text keyText = null;

        private Text valueText = null;

        private String filePathStr = null;

        private DMInterURLDAOObj dmInterURLRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            keyText = new Text();
            valueText = new Text();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePathStr = fileSplit.getPath().getParent().toString();
            this.dmInterURLRuleDAO = new DMInterURLDAOObj(new File(
                    "dm_inter_url"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            keyText.clear();
            valueText.clear();
            String valueStr = value.toString();
            String[] fields = valueStr.split("\t", -1);
            if (filePathStr.contains(LANDPAGE)) {
                keyText.set(fields[1]);
                String pv = fields[2];
                String uv = fields[3];
//                double bounceCount = Double.parseDouble(fields[4]);
//                double sessionCount = Double.parseDouble(fields[5]);
                // BigDecimal bouncerateBigDecimal = new BigDecimal(bounceCount
                // / sessionCount);
                // double bouncerate = bouncerateBigDecimal.setScale(2,
                // BigDecimal.ROUND_HALF_UP).doubleValue();
                valueText.set(LANDPAGE + pv + "\t" + uv + "\t" + fields[4]
                        + "\t" + fields[5]);
            }
            else {
                String url = fields[PvFormatEnum.URL.ordinal()];
                keyText.set(decodeCodedStr(url, "utf-8"));
                String secondId = fields[PvFormatEnum.URL_SECOND_ID.ordinal()];
                String secondName = dmInterURLRuleDAO
                        .getSecondNameBySecondId(secondId);
                if (null != secondName && !"".equalsIgnoreCase(secondName)) {
                    String refferUrl = decodeCodedStr(
                            fields[PvFormatEnum.REFERURL.ordinal()], "utf-8");
                    boolean isInstationReff = refferUrl
                            .indexOf("http://www.funshion.com") == 0
                            || refferUrl.indexOf("http://fs.funshion.com") == 0;
                    if (isInstationReff) {
                        valueText.set(secondName
                                + "\t"
                                + new String(refferUrl.substring("http://"
                                        .length())));
                    }
                    else {
                        if (refferUrl.indexOf("http://") == 0) {
                            refferUrl = new String(
                                    refferUrl.substring("http://".length()));
                        }
                        boolean isNotSingleWeb = refferUrl.indexOf("/") > 0;
                        if (isNotSingleWeb) {
                            refferUrl = refferUrl.substring(0,
                                    refferUrl.indexOf("/"));
                            valueText.set(secondName
                                    + "\t"
                                    + (refferUrl.contains(":") ? new String(
                                            refferUrl.substring(0,
                                                    refferUrl.indexOf(":")))
                                            : refferUrl));
                        }
                    }
                }
            }

            context.write(keyText, valueText);
        }
    }

    public static class SeoImportPageReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        private Text keyText = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            keyText = new Text();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String landPageInfo = null;
            String pvInfo = null;
            keyText.clear();
            for (Text value : values) {
                String valueStr = value.toString();
                if (!"".equalsIgnoreCase(valueStr)) {
                    if (valueStr.contains(LANDPAGE)) {
                        landPageInfo = new String(valueStr.substring(LANDPAGE
                                .length()));
                    }
                    else {
                        pvInfo = valueStr;
                    }
                }
            }

            if (null != landPageInfo && null != pvInfo
                    && !"".equalsIgnoreCase(landPageInfo)
                    && !"".equalsIgnoreCase(pvInfo)) {
                keyText.set(pvInfo + "\t" + landPageInfo);
                context.write(keyText, NullWritable.get());
            }
        }
    }

    /**
     * @throws Exception
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int res = ToolRunner.run(new Configuration(), new SeoImportPageMR(),
                args);
        System.out.println(res);
    }

    /**
     * (非 Javadoc)
     * <p>
     * Title: run
     * </p>
     * <p>
     * Description:
     * </p>
     * 
     * @param args
     * @return
     * @throws Exception
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(SeoImportPageMR.class);
        job.setMapperClass(SeoImportPageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SeoImportPageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String jobName = job.getConfiguration().get(
                MapReduceConfInfoEnum.jobName.getValueStr());
        job.setJobName(jobName);
        String inputPathStr = job.getConfiguration().get(
                MapReduceConfInfoEnum.inputPath.getValueStr());
        System.out.println(inputPathStr);
        String outputPathStr = job.getConfiguration().get(
                MapReduceConfInfoEnum.outPutPath.getValueStr());
        System.out.println(outputPathStr);
        HdfsUtil.deleteDir(outputPathStr);
        int reduceNum = job.getConfiguration().getInt(
                MapReduceConfInfoEnum.reduceNum.getValueStr(), 0);
        System.out.println(MapReduceConfInfoEnum.reduceNum.getValueStr() + ":"
                + reduceNum);
        FileInputFormat.setInputPaths(job, inputPathStr);
        FileOutputFormat.setOutputPath(job, new Path(outputPathStr));
        job.setNumReduceTasks(reduceNum);
        int isInputLZOCompress = job
                .getConfiguration()
                .getInt(MapReduceConfInfoEnum.isInputFormatLZOCompress
                        .getValueStr(),
                        1);
        if (1 == isInputLZOCompress) {
            job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        }
        job.waitForCompletion(true);
        return 0;
    }

}
