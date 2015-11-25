package com.bi.calculate;

/**
 * @author wangxw
 * @descrition 
 * 计算缓冲失败数 和缓冲时间 
 * 输出:总记录数 总缓冲时间   失败记录数
 * 计算平均缓存时间（成功） 失败率
 *  
 */
import jargs.gnu.CmdLineParser.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

public class BufferFail extends Configured implements Tool {

    public static class BufferFailMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private int[] groupByColumns;

        private int[] timeOkColumns;

        private int type;

        public void setup(Context context) throws NumberFormatException {

            try {
                String[] columns = context.getConfiguration().get("groupby")
                        .split(",");
                groupByColumns = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    groupByColumns[i] = Integer.parseInt(columns[i]);

                }
                // 指标列,ok and btime
                columns = context.getConfiguration().get("timeok").split(",");
                timeOkColumns = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    timeOkColumns[i] = Integer.parseInt(columns[i]);

                }
                // type 1:dbuffer fbuffer stuck ;type 2:bootstrap
                type = Integer.parseInt(context.getConfiguration().get("type"));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String[] fields = StringUtil.splitLog(value.toString(), '\t');
                int successTime = processBtm((fields[timeOkColumns[0]]));
                String okType = fields[timeOkColumns[1]];

                int fail = 0;
                if (isFail(type, okType, successTime)) {
                    fail = 1;
                }
                else {
                    fail = 0;
                }

                int success = 0;
                if (isSuccess(type, okType, successTime)) {
                    success = 1;
                }
                else {
                    success = 0;
                    successTime = 0;
                }

                for (String mapOutputKey : getOutputKey(fields)) {
                    StringBuffer mapOutputValue = new StringBuffer();
                    mapOutputValue.append(successTime);
                    mapOutputValue.append("\t");
                    mapOutputValue.append(fail);
                    mapOutputValue.append("\t");
                    mapOutputValue.append(success);
                    context.write(new Text(mapOutputKey), new Text(
                            mapOutputValue.toString()));
                }

            }
            catch(Exception e) {
                // TODO: handle exception
            }

        }

        private boolean isFail(int type, String okType, int btmFormat) {

            try {
                if (type == 1) {
                    if (null == okType)
                        return false;
                    int ok = Integer.parseInt(okType);
                    if (ok == 0)
                        return false;
                    if (ok == -3 && btmFormat < 45000)
                        return false;
                    if (ok == -7 && btmFormat < 45000)
                        return false;
                    return true;
                }
                else if (type == 2) {
                    return btmFormat > 5000;
                }
                return false;

            }
            catch(Exception e) {
                return false;
            }
        }

        private boolean isSuccess(int type, String okType, int btmFormat) {

            try {
                if (type == 1) {
                    if (null == okType)
                        return false;
                    int ok = Integer.parseInt(okType);
                    return ok == 0;
                }
                else if (type == 2) {
                    return btmFormat <= 5000;
                }
                return false;

            }
            catch(Exception e) {
                return false;
            }
        }

        private int processBtm(String origin) {
            try {
                double result = Double.valueOf(origin);
                return ((int) result) >= 0 ? (int) result : 0;
            }
            catch(Exception e) {
                // TODO: handle exception
                new Exception(e.getMessage() + origin).printStackTrace();
                return 0;
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
                sb.append(fields[0] + "\t");
                for (;;) {
                    Integer value = iterator.next();
                    sb.append(value);
                    if (iterator.hasNext()) {
                        sb.append("\t");

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

    public static class BufferFailReducer extends
            Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalUser = 0;
            long failUser = 0;
            long successUser = 0;
            long totalSuccesstime = 0;
            for (Text value : values) {
                String[] field = value.toString().split("\t");
                long successTime = Integer.parseInt(field[0]);
                long fail = Integer.parseInt(field[1]);
                long success = Integer.parseInt(field[2]);
                totalUser++;
                failUser += fail;
                successUser += success;
                totalSuccesstime += successTime;

            }
            // btime,successsum,failsum
            StringBuilder value = new StringBuilder();
            value.append(totalUser);
            value.append("\t");
            value.append(failUser);
            value.append("\t");
            value.append(successUser);
            value.append("\t");
            value.append(totalSuccesstime);
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
                Option idColumnOption = getParser().addHelp(
                        getParser().addStringOption("groupby"),
                        "the group column");
                options.add(idColumnOption);

                Option indexColumnOption = getParser().addHelp(
                        getParser().addStringOption("timeok"),
                        "the time and ok column");
                options.add(indexColumnOption);
                Option type = getParser().addHelp(
                        getParser().addStringOption("type"), "the log type ");
                options.add(type);
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

        nRet = ToolRunner.run(new Configuration(), new BufferFail(),
                paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("groupby", args[2]);
        conf.set("timeok", args[3]);
        conf.set("type", args[4]);
        Job job = new Job(conf, "mobilequality-buffer-index");
        job.setJarByClass(BufferFail.class);
        FileInputFormat.addInputPaths(job, args[0]);
        HdfsUtil.deleteDir(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BufferFailMapper.class);
        job.setReducerClass(BufferFailReducer.class);
        job.setNumReduceTasks(60);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
