package com.bi.calculate;

/**
 * 
 * 计算安桌启动观看的mac
 * 1,计算启动观看了推送媒体的mac
 * 2,计算启动观看了所有媒体的mac
 */
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
import com.bi.common.util.MidUtil;
import com.bi.common.util.StringUtil;

/**
 * 
 * @ClassName: AndriodBootRate
 * @Description:
 * @author wang
 * @date 2013-9-25 上午12:11:35
 */
public class AndriodWatchDownMac extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodWatchDownMacMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePath;

        private static final int PLAT_INDEX = 2;

        private MidUtil midUtil;

        private int type;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            String midPath = context.getConfiguration().get("mid");
            type = Integer.parseInt(context.getConfiguration().get("type"));
            midUtil = MidUtil.getInstance(midPath);

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
            if (fromBootstrap(filePath)) {

                int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                        .ordinal()]);
                boolean desktop = (btype == 7);
                boolean notice = (btype == 3);
                if (!desktop && !notice)
                    return;
                mac = fields[FormatBootStrapEnum.MAC.ordinal()];
                fileTag = 1;

            }
            else if (fromFbuffer(filePath)) {
                String mid = fields[FormatFbufferEnum.MID.ordinal()];
                if (!midOk(plat, mid) && !(type == 2))
                    return;
                mac = fields[FormatFbufferEnum.MAC.ordinal()];
                fileTag = 2;

            }
            else if (fromDownload(filePath)) {
                String mid = fields[FormatDownloadEnum.MID.ordinal()];
                if (!midOk(plat, mid) && !(type == 2))
                    return;
                mac = fields[FormatDownloadEnum.MAC.ordinal()];
                fileTag = 3;

            }

            context.write(new Text(mac),
                    new Text(fileTag + "\t" + value.toString()));

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

        private boolean midOk(int plat, String mid) {
            return midUtil.containMid(plat, mid);

        }

    }

    public static class AndriodWatchDownMacReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean fromBootstrap = false;
            int fbufferExsitTag = 0;
            int downloadExsitTag = 0;
            List<String> list = new ArrayList<String>(30);
            for (Text value : values) {

                int fileTag = Integer.parseInt(StringUtil.splitLog(
                        value.toString(), '\t')[0]);
                switch (fileTag) {
                case 1:
                    list.add(processOutputValue(value.toString()));
                    fromBootstrap = true;
                    break;
                case 2:
                    fbufferExsitTag = 1;
                    break;
                case 3:
                    downloadExsitTag = 1;
                    break;
                default:
                    break;
                }

            }

            if (fromBootstrap
                    && (fbufferExsitTag == 1 || downloadExsitTag == 1))
                for (String outputValue : list)
                    context.write(new Text(outputValue), new Text(
                            fbufferExsitTag + "\t" + downloadExsitTag));

        }

        private String processOutputValue(String origin) {
            String[] fields = StringUtil.splitLog(origin, '\t');
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < fields.length; i++) {
                sb.append(fields[i]);
                if (i != fields.length)
                    sb.append("\t");
            }
            return sb.toString();

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

        AbstractCmdParamParse paramParse = new BaseCmdParamParse() {
            @Override
            public Option[] getOptions() {
                List<Option> options = new ArrayList<Option>(0);
                Option type = getParser().addHelp(
                        getParser().addStringOption("type"), "the mac type");
                options.add(type);
                Option midOption = getParser()
                        .addHelp(getParser().addStringOption("mid"),
                                "the mid file path");
                options.add(midOption);

                return options.toArray(new Option[options.size()]);

            }

            @Override
            public String[] getParams() {
                // TODO Auto-generated method stub

                String[] params = super.getParams();
                int length = params.length;
                // file param must be the first
                String[] convertParam = new String[length + 2];
                System.arraycopy(params, 0, convertParam, 2, length);
                convertParam[0] = "-files";
                convertParam[1] = params[length - 1];
                return convertParam;
            }
        };
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new AndriodWatchDownMac(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("type", args[2]);
        conf.set("mid", args[3]);
        Job job = new Job(conf, "pushreach-andriod-watch-mac-extract");
        job.setJarByClass(AndriodWatchDownMac.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodWatchDownMacMapper.class);
        job.setReducerClass(AndriodWatchDownMacReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
