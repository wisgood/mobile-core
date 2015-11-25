package com.bi.calculate;

/**
 * 
 * 计算新增用户中属于push的用户数
 * 输入日志为 : /dw/logs/4_mobile_platform/5_pushreach/format/app_bootstrap/和/dw/logs/mobile/result/dayuser/new
 * 输入日志格式为： 日期 + 平台 +mac
 * 输出格式为：日期+平台+用户数
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

public class AndriodPushNewUser extends Configured implements Tool {

    private static char SEPERATOR = '\t';

    public static class AndriodPushNewUserMapper extends
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
            String mac = fields[FormatBootStrapEnum.MAC.ordinal()];

            for (String mapOutputKey : getOutputKey(fields)) {
                context.write(new Text(mapOutputKey), new Text(mac));
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

    public static class AndriodPushNewUserReducer extends
            Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> userMac = new HashSet<String>();
            for (Text value : values) {
                userMac.add(value.toString());

            }
            context.write(key, new Text(String.valueOf(userMac.size())));

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

        nRet = ToolRunner.run(new Configuration(), new AndriodPushNewUser(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("groupby", args[2]);
        Job job = new Job(conf, "andriod-pushnewuser");
        job.setJarByClass(AndriodPushNewUser.class);
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
