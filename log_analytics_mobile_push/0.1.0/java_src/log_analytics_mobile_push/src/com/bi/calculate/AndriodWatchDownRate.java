package com.bi.calculate;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: AndriodBootRate.java 
 * @Package com.bi.calculate 
 * @Description: 安卓 启动观看率和启动观看用户率计算(汇总 桌面通知 通知栏)
 * @author wangxw
 * @date 2013-9-26 下午2:44:13 
 * @input:   bootstrap pushreach日志 
 * @output: 维度 (日期 + 小时 ＋平台) 
 * @output:     汇总启动观看记录数 汇总启动观看用户数 
 * @output: 桌面通知启动观看记录数 桌面通知启动观看用户数 
 * @output: 通知栏启动观看记录数 通知栏启动观看用户数 
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

/**
 * 
 * @ClassName: AndriodWatchDownRate
 * @Description:
 * @author wang
 * @date 2013-9-25 上午12:11:35
 */
public class AndriodWatchDownRate extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodWatchDownRateMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private int[] groupByColumns;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
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
            int btype = Integer.parseInt(fields[FormatBootStrapEnum.BTYPE
                    .ordinal()]);
            String mac = fields[FormatBootStrapEnum.MAC.ordinal()];
            boolean desktop = (btype == 7);
            boolean notice = (btype == 3);
            int totalwatchRecords = 0;
            int desktopWatchRecords = 0;
            int noticeWatchRecords = 0;
            if (desktop) {
                desktopWatchRecords = 1;
                totalwatchRecords = 1;
            }
            else if (notice) {
                noticeWatchRecords = 1;
                totalwatchRecords = 1;
            }
            String outputValue = new StringBuilder().append(totalwatchRecords)
                    .append("\t").append(desktopWatchRecords).append("\t")
                    .append(noticeWatchRecords).append("\t").append(mac)
                    .toString();

            for (String mapOutputKey : getOutputKey(fields)) {
                context.write(new Text(mapOutputKey), new Text(outputValue));
            }

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

    public static class AndriodWatchDownRateReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long totalWatchRecords = 0;
            long desktopWatchRecords = 0;
            long noticeWatchRecords = 0;

            Set<String> totalWatchMac = new HashSet<String>();
            Set<String> desktopWatchMac = new HashSet<String>();
            Set<String> noticeWatchMac = new HashSet<String>();
            for (Text value : values) {
                String[] field = StringUtil.splitLog(value.toString(),
                        SEPERATOR);
                String mac = field[3];
                long currentWatchRecords = Long.parseLong(field[0]);
                totalWatchRecords += currentWatchRecords;
                long currentDesktopWatchRecords = Long.parseLong(field[1]);
                desktopWatchRecords += currentDesktopWatchRecords;
                long currentNoticeWatchRecords = Long.parseLong(field[2]);
                noticeWatchRecords += currentNoticeWatchRecords;
                if (currentWatchRecords == 1)
                    totalWatchMac.add(mac);
                if (currentDesktopWatchRecords == 1)
                    desktopWatchMac.add(mac);
                if (currentNoticeWatchRecords == 1)
                    noticeWatchMac.add(mac);

            }
            StringBuilder value = new StringBuilder();
            value.append(totalWatchRecords);
            value.append(SEPERATOR);
            value.append(totalWatchMac.size());
            value.append(SEPERATOR);
            value.append(desktopWatchRecords);
            value.append(SEPERATOR);
            value.append(desktopWatchMac.size());
            value.append(SEPERATOR);
            value.append(noticeWatchRecords);
            value.append(SEPERATOR);
            value.append(noticeWatchMac.size());
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

        nRet = ToolRunner.run(new Configuration(), new AndriodWatchDownRate(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("groupby", args[2]);
        Job job = new Job(conf, "pushreach-andriod-watchrate-cal");
        job.setJarByClass(AndriodWatchDownRate.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AndriodWatchDownRateMapper.class);
        job.setReducerClass(AndriodWatchDownRateReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
