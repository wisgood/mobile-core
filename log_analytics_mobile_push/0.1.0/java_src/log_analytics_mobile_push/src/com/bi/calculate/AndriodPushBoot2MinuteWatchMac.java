package com.bi.calculate;

/** 
 * 
 * 计算安桌PUSH启动2分钟内的观看（下载）的mac
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.logenum.FormatDownloadEnum;
import com.bi.common.logenum.FormatFbufferEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

/**
 * 
 * @ClassName: AndriodBootRate
 * @Description:
 * @author wang
 * @date 2013-9-25 上午12:11:35
 */
public class AndriodPushBoot2MinuteWatchMac extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodPushBoot2MinuteWatchMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePath;

        private static final int PLAT_INDEX = 2;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtil.splitLog(line, SEPERATOR);
            int plat = Integer.parseInt(fields[PLAT_INDEX]);
            boolean ios = (plat == 3) || (plat == 4);
            if (ios)
                return;
            int fileTag = 0;
            String mac = "";
            String timeStamp = "";
            if (fromBootstrap(filePath)) {

                int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                        .ordinal()]);
                boolean desktop = (btype == 7);
                boolean notice = (btype == 3);
                if (!desktop && !notice)
                    return;
                mac = fields[FormatBootStrapEnum.MAC.ordinal()];
                fileTag = 1;
                timeStamp = fields[FormatBootStrapEnum.TIMESTAMP.ordinal()];

            }
            else if (fromFbuffer(filePath)) {
                mac = fields[FormatFbufferEnum.MAC.ordinal()];
                fileTag = 2;
                timeStamp = fields[FormatFbufferEnum.TIMESTAMP.ordinal()];

            }
            else if (fromDownload(filePath)) {
                mac = fields[FormatDownloadEnum.MAC.ordinal()];
                fileTag = 3;
                timeStamp = fields[FormatDownloadEnum.TIMESTAMP.ordinal()];

            }

            context.write(new Text(mac), new Text(fileTag + "\t" + timeStamp
                    + "\t" + value.toString()));

        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }

        private boolean fromFbuffer(String filePath) {
            return filePath.toLowerCase().contains("fbuffer".toLowerCase());
        }

        private boolean fromDownload(String filePath) {
            return filePath.toLowerCase().contains("download".toLowerCase());
        }

    }

    public static class AndriodPushBoot2MinuteWatchMacReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean fromBootstrap = false;
            List<String> list = new ArrayList<String>(30);
            boolean watchOrDown = false;
            List<Long> bootstrapTime = new ArrayList<Long>(10);
            List<Long> watchTime = new ArrayList<Long>(10);
            for (Text value : values) {

                String[] fields = StringUtil.splitLog(value.toString(), '\t');
                int fileTag = Integer.parseInt(fields[0]);
                switch (fileTag) {
                case 1:
                    list.add(processOutputValue(value.toString()));
                    fromBootstrap = true;
                    bootstrapTime.add(Long.parseLong(fields[1]));
                    break;
                case 2:
                    watchTime.add(Long.parseLong(fields[1]));
                    break;
                case 3:
                    watchTime.add(Long.parseLong(fields[1]));
                    break;
                default:
                    break;
                }

            }

            for (int i = 0; i < bootstrapTime.size(); i++)
                for (int j = 0; j < watchTime.size(); j++) {
                    long boottime = bootstrapTime.get(i);
                    long watchtime = watchTime.get(j);
                    if (withIn2Minute(boottime, watchtime)) {
                        watchOrDown = true;
                        break;

                    }

                }

            if (fromBootstrap && watchOrDown)
                for (String outputValue : list)
                    context.write(new Text(outputValue), NullWritable.get());

        }

        private String processOutputValue(String origin) {
            String[] fields = StringUtil.splitLog(origin, '\t');
            StringBuilder sb = new StringBuilder();
            for (int i = 2; i < fields.length; i++) {
                sb.append(fields[i]);
                if (i != fields.length)
                    sb.append("\t");
            }
            return sb.toString();

        }

        private boolean withIn2Minute(long bootstrapTime, long anthoTime) {
            int secondsPerMinute = 60;
            return anthoTime - bootstrapTime <= 2 * secondsPerMinute
                    && anthoTime - bootstrapTime >= 0;
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

        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(),
                new AndriodPushBoot2MinuteWatchMac(), paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "andriod-pushboot-2miniute-watch-mac");
        job.setJarByClass(AndriodPushBoot2MinuteWatchMac.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodPushBoot2MinuteWatchMacMapper.class);
        job.setReducerClass(AndriodPushBoot2MinuteWatchMacReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
