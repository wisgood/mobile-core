package com.bi.analytics.srcdest.MajorPageAnalysis;

import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import com.bi.common.dm.pojo.fieldEnum.DMMajorPageUrlEnum;
import com.bi.common.dm.pojo.fieldEnum.DMURLSrcPageEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.util.StringDecodeFormatUtil;
import com.bi.log.pv.format.dataenum.PvFormatEnum;

public class MajorPageDest extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private static final String DEST_PAGETYPE = "3";

    public static class MajorPageDestMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] fields = line.split(SEPARATOR);

                // Major Page
                if (fields.length == DMMajorPageUrlEnum.URL_DESC.ordinal() + 1) {

                    String urlStr = StringDecodeFormatUtil
                            .urlNormalizer(fields[DMMajorPageUrlEnum.URL
                                    .ordinal()]);
                    context.write(new Text(urlStr), new Text(0 + SEPARATOR
                            + line));

                }

                // Pv origin
                else {
                    String urlStr = null;
                    try{
                        urlStr = StringDecodeFormatUtil
                            .urlNormalizer(fields[PvFormatEnum.URL.ordinal()]);
                    }
                    catch(Exception e){
                        System.out.println("fields length:" + fields.length);
                        System.out.println("fields string:" + fields.toString());
                    }
                    String dateIdStr = fields[PvFormatEnum.DATE_ID.ordinal()];
                    String fckStr = fields[PvFormatEnum.FCK.ordinal()];
                    String referUrlStr = StringDecodeFormatUtil
                            .urlNormalizer(fields[PvFormatEnum.REFERURL
                                    .ordinal()]);

                    String referThirdIdStr = fields[PvFormatEnum.REFER_THIRD_ID
                            .ordinal()];

                    int referThirdId = 0;
                    try {
                        referThirdId = Integer.parseInt(referThirdIdStr);

                    }
                    catch(NumberFormatException e) {
                        return;
                    }
                    
                    if (referThirdId < 1000) {
                        context.write(new Text(referUrlStr), new Text(1 + SEPARATOR
                                + DEST_PAGETYPE + SEPARATOR + fckStr
                                + SEPARATOR + dateIdStr + SEPARATOR
                                + urlStr));
                    }
                }
            }
            catch(ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }

            // }
        }
    }

    public static class MajorPageDestReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String DmMajorPageStr = "";
            String dateIdStr = null;

            List<String> pvInfoList = new ArrayList<String>();

            Map<String, List<String>> ReferFckMap = new HashMap<String, List<String>>();

            for (Text val : values) {
                String value = val.toString().trim();
                if (value.startsWith("0")) {
                    DmMajorPageStr = value;
                }
                else {
                    pvInfoList.add(value);
                }
            }

            if (null == DmMajorPageStr || "".equals(DmMajorPageStr)) {
                return;
            }
            else {
                for (String pvInfoStr : pvInfoList) {
                    String[] fields = pvInfoStr.split(SEPARATOR);
                    String sdTypeIdStr = fields[1];
                    String fckStr = fields[2];
                    dateIdStr = fields[3];
                    String urlStr = null;
                    try {
                        urlStr = fields[4];
                    }
                    catch(Exception e) {
                        continue;
                    }
                    
                    if (!ReferFckMap.containsKey(sdTypeIdStr + SEPARATOR
                            + urlStr)) {
                        List<String> fckList = new ArrayList<String>();
                        fckList.add(fckStr);
                        ReferFckMap.put(sdTypeIdStr + SEPARATOR + urlStr,
                                fckList);
                    }
                    else {
                        ReferFckMap.get(sdTypeIdStr + SEPARATOR + urlStr)
                                .add(fckStr);
                    }
                }
            }

            for (Map.Entry<String, List<String>> entry : ReferFckMap.entrySet()) {
                Set<String> fckSet = new HashSet<String>();
                long PV = entry.getValue().size();
                for (String fckInfoStr : entry.getValue()) {
                    fckSet.add(fckInfoStr);
                }
                long UV = fckSet.size();
                context.write(new Text(dateIdStr + SEPARATOR + key.toString()),
                        new Text(entry.getKey() + SEPARATOR + PV + SEPARATOR
                                + UV));
            }

            /*
             * Iterator iter = referFckMap.entrySet().iterator(); while
             * (iter.hasNext()) {
             * 
             * Set<String> fckSet = new HashSet<String>();
             * 
             * Map.Entry<String, List<String>> entry = (Map.Entry<String,
             * List<String>>) iter .next(); long PV = entry.getValue().size();
             * for(String fckInfoStr : entry.getValue()){
             * fckSet.add(fckInfoStr); } long UV = fckSet.size();
             * 
             * context.write(new Text(dateIdStr + SEPARATOR + key.toString()),
             * new Text( PV+ SEPARATOR + UV )); }
             */
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

        nRet = ToolRunner.run(new Configuration(), new MajorPageDest(),
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
        Job job = new Job(conf, "MajorPageDest");
        job.setJarByClass(MajorPageDest.class);
        // System.out.println("inputPaths:"+args[0]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MajorPageDestMapper.class);
        job.setReducerClass(MajorPageDestReducer.class);
        // job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(50);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
