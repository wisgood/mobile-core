package com.bi.calculate;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: AndriodBootRate.java 
 * @Package com.bi.calculate 
 * @Description: 安卓 启动率和启动用户率计算(汇总 桌面通知 通知栏)
 * @author wangxw
 * @date 2013-9-26 下午2:44:13 
 * @input:   bootstrap pushreach日志 
 * @output: 维度 (日期 + 小时 ＋平台) 
 * @output: 汇总到达记录数     汇总到达成功记录数   汇总启动记录数 汇总达到用户数 汇总启动成功用户数 
 * @output: 桌面通知到达记录数       桌面通知启动记录数   桌面通知到达用户数    桌面通知启动用户数 
 * @output: 通知栏到达记录数         通知栏启动记录数     通知栏到达用户数      通知栏启动用户数 
 * @inputFormat:DATE_ID HOUR_ID PLAT_ID VERSION_ID  MAC IP  BTYPE   TIMESTAMP
 */
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.logenum.FormatPushreachEnum;
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
public class AndriodBootRate extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodBootRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePath;

        private static final int PLAT_INDEX = 2;

        private int[] groupByColumns;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();
            String[] columns = context.getConfiguration().get("groupby")
                    .split(",");
            groupByColumns = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                groupByColumns[i] = Integer.parseInt(columns[i]);
            }

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtil.splitLog(line, SEPERATOR);
            int plat = Integer.parseInt(fields[PLAT_INDEX]);
            boolean ios = (plat == 3) || (plat == 4);
            if (ios)
                return;
            int totalReachRecords = 0;
            int totalReachSucRecords = 0;
            int totalBootRecords = 0;
            int desktopReachRecords = 0;
            int desktopBootRecords = 0;
            int noticeReachRecords = 0;
            int noticeBootRecords = 0;
            String mac = null;
            if (fromBootstrap(filePath)) {

                int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                        .ordinal()]);
                mac = fields[FormatBootStrapEnum.MAC.ordinal()];
                boolean desktop = (btype == 7);
                boolean notice = (btype == 3);
                if (desktop) {
                    desktopBootRecords = 1;
                    totalBootRecords = 1;
                }
                else if (notice) {
                    noticeBootRecords = 1;
                    totalBootRecords = 1;
                }

            }
            else if (fromPushreach(filePath)) {
                totalReachRecords = 1;
                int ok = Integer.parseInt(fields[FormatPushreachEnum.OK
                        .ordinal()]);
                if (ok == 1)
                    totalReachSucRecords = 1;
                int messtype = Integer
                        .parseInt(fields[FormatPushreachEnum.MESSAGETYPE
                                .ordinal()]);

                mac = fields[FormatPushreachEnum.MAC.ordinal()];
                boolean desktop = (messtype == 2) || (messtype == 3);
                boolean notice = (messtype == 1) || (messtype == 3);
                if (desktop) {
                    desktopReachRecords = 1;
                }
                else if (notice) {
                    noticeReachRecords = 1;
                }

            }
            String outputValue = new StringBuilder().append(totalReachRecords)
                    .append("\t").append(totalBootRecords).append("\t")
                    .append(desktopReachRecords).append("\t")
                    .append(desktopBootRecords).append("\t")
                    .append(noticeReachRecords).append("\t")
                    .append(noticeBootRecords).append("\t").append(mac)
                    .append("\t").append(totalReachSucRecords).toString();

            for (String mapOutputKey : getOutputKey(fields)) {
                context.write(new Text(mapOutputKey), new Text(outputValue));
            }

        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }

        private boolean fromPushreach(String filePath) {
            return filePath.toLowerCase().contains("pushreach".toLowerCase());
        }

        private List<String> getOutputKey(String[] fields) {

            List<String> list = new ArrayList<String>(groupByColumns.length);

            int length = groupByColumns.length;
            int max = 1 << length - 1;
            for (int i = 0; i < max; i++) {
                Map<Integer, Integer> map = new HashMap<Integer, Integer>();
                for (int k = 1; k < length; k++) {
                    map.put(new Integer(groupByColumns[k]), new Integer(
                            fields[groupByColumns[k]]));
                }
                for (int j = 0; j < length - 1; j++) {
                    if ((i & (1 << j)) != 0) {
                        map.put(new Integer(groupByColumns[j + 1]),
                                new Integer("-999"));
                    }
                }

                Iterator<Integer> iterator = map.values().iterator();
                if (!iterator.hasNext()) {
                    return list;
                }
                StringBuilder sb = new StringBuilder();
                sb.append(fields[0] + SEPERATOR);
                for (;;) {
                    Integer value = iterator.next();
                    sb.append(value);
                    if (iterator.hasNext()) {
                        sb.append(SEPERATOR);

                    }
                    else {
                        break;
                    }
                }
                list.add(sb.toString());
            }
            return list;
        }
    }

    public static class AndriodBootRateReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long totalReachRecords = 0;
            long totalReachSucRecords = 0;
            long totalBootRecords = 0;
            long desktopReachRecords = 0;
            long desktopBootRecords = 0;
            long noticeReachRecords = 0;
            long noticeBootRecords = 0;

            Set<String> totalReachMacSet = new HashSet<String>();
            Set<String> totalBootMacSet = new HashSet<String>();
            Set<String> desktopReachMacSet = new HashSet<String>();
            Set<String> desktopBootSet = new HashSet<String>();
            Set<String> noticeReachMacSet = new HashSet<String>();
            Set<String> noticeBootMacSet = new HashSet<String>();

            for (Text value : values) {
                String[] field = StringUtil.splitLog(value.toString(),
                        SEPERATOR);
                ;
                String mac = field[6];
                long currentReachRecords = Long.parseLong(field[0]);
                totalReachRecords += currentReachRecords;

                long currentBootRecords = Long.parseLong(field[1]);
                totalBootRecords += currentBootRecords;

                long currentDesktopReachRecords = Long.parseLong(field[2]);
                desktopReachRecords += currentDesktopReachRecords;

                long currentDesktopBootRecords = Long.parseLong(field[3]);
                desktopBootRecords += currentDesktopBootRecords;

                long currentNoticeReachRecords = Long.parseLong(field[4]);
                noticeReachRecords += currentNoticeReachRecords;

                long currentNoticeBootRecords = Long.parseLong(field[5]);
                noticeBootRecords += currentNoticeBootRecords;

                totalReachSucRecords += Long.parseLong(field[7]);

                if (currentReachRecords == 1)
                    totalReachMacSet.add(mac);
                if (currentBootRecords == 1)
                    totalBootMacSet.add(mac);
                if (currentDesktopReachRecords == 1)
                    desktopReachMacSet.add(mac);
                if (currentDesktopBootRecords == 1)
                    desktopBootSet.add(mac);
                if (currentNoticeReachRecords == 1)
                    noticeReachMacSet.add(mac);
                if (currentNoticeBootRecords == 1)
                    noticeBootMacSet.add(mac);

            }
            StringBuilder value = new StringBuilder();
            value.append(totalReachRecords);
            value.append(SEPERATOR);
            value.append(totalReachSucRecords);
            value.append(SEPERATOR);
            value.append(totalBootRecords);
            value.append(SEPERATOR);
            value.append(totalReachMacSet.size());
            value.append(SEPERATOR);
            value.append(totalBootMacSet.size());
            value.append(SEPERATOR);
            value.append(desktopReachRecords);
            value.append(SEPERATOR);
            value.append(desktopBootRecords);
            value.append(SEPERATOR);
            value.append(desktopReachMacSet.size());
            value.append(SEPERATOR);
            value.append(desktopBootSet.size());
            value.append(SEPERATOR);
            value.append(noticeReachRecords);
            value.append(SEPERATOR);
            value.append(noticeBootRecords);
            value.append(SEPERATOR);
            value.append(noticeReachMacSet.size());
            value.append(SEPERATOR);
            value.append(noticeBootMacSet.size());
            context.write(key, new Text(value.toString()));

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
                Option groupByColumnOption = getParser().addHelp(
                        getParser().addStringOption("groupby"),
                        "the group column");
                options.add(groupByColumnOption);
                return options.toArray(new Option[options.size()]);

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

        nRet = ToolRunner.run(new Configuration(), new AndriodBootRate(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("groupby", args[2]);
        Job job = new Job(conf, "pushreach-andriod-bootrate-cal");
        job.setJarByClass(AndriodBootRate.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodBootRateMapper.class);
        // job.setCombinerClass(AndriodBootRateReducer.class);
        job.setReducerClass(AndriodBootRateReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
