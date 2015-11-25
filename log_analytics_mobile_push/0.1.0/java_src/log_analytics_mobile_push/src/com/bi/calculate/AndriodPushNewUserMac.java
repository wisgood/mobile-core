package com.bi.calculate;

/**
 * 
 * 抽取新增用户中属于push的mac地址
 * 输入日志为 : /dw/logs/4_mobile_platform/5_pushreach/format/app_bootstrap/和/dw/logs/mobile/result/dayuser/new
 * 输出格式为： 日期 + 平台 +mac
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
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

public class AndriodPushNewUserMac extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodPushNewUserMapper extends
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
            else {
                mac = fields[6];
                fileTag = 2;
            }

            context.write(new Text(mac),
                    new Text(fileTag + "\t" + value.toString()));

        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }
    }

    public static class AndriodPushNewUserReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            boolean fromBootstrap = false;
            boolean fromNewMac = false;
            List<String> list = new ArrayList<String>(30);
            for (Text value : values) {

                String[] fields = StringUtil.splitLog(value.toString(), '\t');
                int fileTag = Integer.parseInt(fields[0]);
                switch (fileTag) {
                case 1:
                    fromBootstrap = true;
                    list.add(processOutputValue(value.toString()));
                    break;
                case 2:
                    fromNewMac = true;
                    break;
                default:
                    break;
                }

            }

            if (fromBootstrap && fromNewMac) {
                for (String outputValue : list)
                    context.write(new Text(outputValue), NullWritable.get());
            }

        }

        private String processOutputValue(String origin) {
            String[] fields = StringUtil.splitLog(origin, '\t');
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < fields.length; i++) {
                sb.append(fields[i]);
                if (i != fields.length - 1)
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

        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new AndriodPushNewUserMac(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "andriod-pushnewusermac");
        job.setJarByClass(AndriodPushNewUserMac.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodPushNewUserMapper.class);
        job.setReducerClass(AndriodPushNewUserReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
