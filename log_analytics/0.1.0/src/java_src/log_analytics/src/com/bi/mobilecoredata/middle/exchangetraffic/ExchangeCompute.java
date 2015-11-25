package com.bi.mobilecoredata.middle.exchangetraffic;

import jargs.gnu.CmdLineParser.Option;

import java.io.File;
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

import com.bi.comm.paramparse.AbstractCommandParamParse;
import com.bi.comm.util.PlatTypeFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMPlatyRuleDAOImpl;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * 
 * @ClassName: ExchangeCompute
 * @Description: 用来计算换进换出流量的MR程序
 * @Description: 程序输入 appspread spread日志
 * @Description: 程序输出：日期 渠道 平台 换出点击 换出独立IP 换回点击量 换回独立IP
 * @author wangxw
 * @date 2013-7-7 下午7:23:33
 */
public class ExchangeCompute extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    private enum AppSpread {
        TIMESTAMP, IP, PLAT, APPTYPE, MAC, APPNAME, CHANNEL
    }

    private enum Spread {
        TIMESTAMP, IP, PLAT, CHANNEL, ENVIMENTIP, URL1, URL2, URL3
    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */

    public static class ExchangeComputeMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        private Path filePath;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent();

            if (isLocalRunMode(context)) {
                String dmMobilePlayFilePath = context.getConfiguration().get(
                        ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name());
                dmPlatyRuleDAO.parseDMObj(new File(dmMobilePlayFilePath));
            }
            else {
                File dmMobilePlayFile = new File(ConstantEnum.DM_MOBILE_PLATY
                        .name().toLowerCase());
                dmPlatyRuleDAO.parseDMObj(dmMobilePlayFile);
            }
        }

        private boolean isLocalRunMode(Context context) {
            String mapredJobTrackerMode = context.getConfiguration().get(
                    "mapred.job.tracker");
            if (null != mapredJobTrackerMode
                    && ConstantEnum.LOCAL.name().equalsIgnoreCase(
                            mapredJobTrackerMode)) {
                return true;
            }
            return false;
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");

            String date;
            String channel;
            String plat;
            String tag;
            String ip;

            String timestampInfoStr = fields[0];
            java.util.Map<ConstantEnum, String> formatTimesMap = TimestampFormatUtil
                    .formatTimestamp(timestampInfoStr);
            // data
            date = formatTimesMap.get(ConstantEnum.DATE_ID);
            if (fromAppSpread(filePath)) {
                if (fields.length < 7) {
                    return;
                }
                String appType = fields[AppSpread.APPTYPE.ordinal()];
                if (!"vv".equals(appType))
                    return;

                plat = fields[AppSpread.PLAT.ordinal()];
                channel = fields[AppSpread.CHANNEL.ordinal()];
                tag = "appspread";
                ip = fields[AppSpread.IP.ordinal()];
            }
            else {
                if (fields.length < 4) {
                    return;
                }
                plat = fields[Spread.PLAT.ordinal()];
                channel = fields[Spread.CHANNEL.ordinal()];
                tag = "spread";
                ip = fields[Spread.IP.ordinal()];
            }
            try {
                plat = String.valueOf(dmPlatyRuleDAO
                        .getDMOjb(PlatTypeFormatUtil.getFormatPlatType(plat)));

            }
            catch(Exception e) {
                // e.printStackTrace();
                return;
            }

            if (!isNumeric(channel))
                return;
            context.write(new Text(date + SEPARATOR + channel + SEPARATOR
                    + plat), new Text(new Text(tag + SEPARATOR + ip)));
        }

        private boolean fromAppSpread(Path filePath) {
            return filePath.toString().contains("appspread");
        }

        public static boolean isNumeric(String str) {
            if (null == str)
                return false;
            for (int i = str.length(); --i >= 0;) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

    }

    public static class ExchangeComputeReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            long exchangeOutNum = 0;
            long exchangeInNum = 0;
            Set<String> exchangeOutIpSet = new HashSet<String>();
            Set<String> exchangeInIpSet = new HashSet<String>();

            for (Text value : values) {
                String[] fields = value.toString().split(SEPARATOR);
                String tag = fields[0];
                String ip = fields[1];
                if ("appspread".equals(tag)) {
                    exchangeOutNum++;
                    exchangeOutIpSet.add(ip);
                }
                else {
                    exchangeInNum++;
                    exchangeInIpSet.add(ip);

                }

            }

            context.write(new Text(key.toString()), new Text(exchangeOutNum
                    + SEPARATOR + exchangeOutIpSet.size() + SEPARATOR
                    + exchangeInNum + SEPARATOR + exchangeInIpSet.size()));

        }

    }

    /**
     * @throws Exception
     * 
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

        String[] params = paramParse.getParams();
        List<String> list = new ArrayList<String>();
        list.add("-files");
        list.add(params[2]);
        list.add(params[0]);
        list.add(params[1]);
        nRet = ToolRunner.run(new Configuration(), new ExchangeCompute(),
                list.toArray(new String[list.size()]));
        System.out.println(nRet);

    }

    static class ParamParse extends AbstractCommandParamParse {

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
            List<Option> options = new ArrayList<Option>(0);
            Option option = getParser().addHelp(
                    getParser().addStringOption("files"),
                    "confige file to resove dimention");
            options.add(option);
            return options.toArray(new Option[options.size()]);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_exchange");
        job.setJarByClass(ExchangeCompute.class);
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        for (String path : args[0].split(",")) {
            try {
                FileInputFormat.addInputPath(job, new Path(path));
            }
            catch(IOException e) {
            }

        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ExchangeComputeMapper.class);
        job.setReducerClass(ExchangeComputeReducer.class);
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
