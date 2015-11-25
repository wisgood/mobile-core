package com.bi.mobilecoredata.middle.user;

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

//计算离线  在线 
public class UserCompute extends Configured implements Tool {

    private enum BootStrap {
        DATE_ID, HOUR_ID, PLAT_ID, VERSION_ID, QUDAO_ID, BOOT_TYPE, MACCLEAN, PROVINCE_ID, CITY_ID, ISP_ID, OK_TYPE, VERSION_STR, TIMESTAMP, IP, DEV, MAC, VER, NT, BTYPE, BTIME, OK, SR, MEM, TDISK, FDISK, SID, RT, IPHONEIP, BROKEN, IMEI;
    }

    private enum Exit {
        DATE_ID,HOUR_ID,PLAT_ID,VERSION_ID,QUDAO_ID,CITY_ID,MACCLEAN,PROVINCE_ID,ISP_ID,TIMESTAMP, IP, DEV, MAC, VER, NT, USETM,TN, SID, RT,IPHONEIP;
    }

    public static class UserComputeMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String type;

        private Path filePath;

        private AbstractDMDAO<String, Integer> dmPlatyRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            type = context.getConfiguration().get("type");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath();
            dmPlatyRuleDAO = new DMPlatyRuleDAOImpl<String, Integer>();
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
            // data
            String date = fields[BootStrap.DATE_ID.ordinal()];
            // mac
            String mac = fields[BootStrap.MAC.ordinal()];
            // plat
            String plat = null;
            if ("0".equals(type)) {
                // 计算所有用户
                if (filePath.toString().contains("bootstrap")) {
                    plat = fields[BootStrap.PLAT_ID.ordinal()];

                }
                else {
                    plat = fields[Exit.PLAT_ID.ordinal()];
                }
            }
            else if ("1".equals(type)) {
                // 计算在线用户
                if (filePath.toString().contains("bootstrap")) {
                    if (!ntOK(fields[BootStrap.NT.ordinal()], "1"))
                        return;
                    plat = getPlat(fields[BootStrap.DEV.ordinal()]);

                }
                else {
                    if (!ntOK(fields[Exit.NT.ordinal()], "1"))
                        return;
                    plat = getPlat(fields[Exit.DEV.ordinal()]);
                }
            }
            else if ("2".equals(type)) {
                // 计算离线用户
                if (filePath.toString().contains("bootstrap")) {
                    if (!ntOK(fields[BootStrap.NT.ordinal()], "2"))
                        return;
                    plat = getPlat(fields[BootStrap.DEV.ordinal()]);

                }
                else {
                    if (!ntOK(fields[Exit.NT.ordinal()], "2"))
                        return;
                    plat = getPlat(fields[Exit.DEV.ordinal()]);
                }

            }
            context.write(new Text(date + "\t" + plat), new Text(mac));

        }

        private boolean ntOK(String nt, String tag) {
            if ("1".equals(tag))
                return "1".equals(nt) || "2".equals(nt) || "0".equals(nt);
            else if ("2".equals(tag))
                return "-1".equals(nt);
            return false;
        }

        private String getPlat(String plat) {
            try {
                return String.valueOf(dmPlatyRuleDAO
                        .getDMOjb(PlatTypeFormatUtil.getFormatPlatType(plat)));
            }
            catch(Exception e) {
                return null;
            }

        }
    }

    public static class UserComputeReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> macSet = new HashSet<String>();
            for (Text value : values) {
                macSet.add(value.toString());
            }

            context.write(new Text(key.toString()),
                    new Text(String.valueOf(macSet.size())));

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
        list.add(params[3]);
        list.add(params[0]);
        list.add(params[1]);
        list.add(params[2]);
        nRet = ToolRunner.run(new Configuration(), new UserCompute(),
                list.toArray(new String[list.size()]));
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "2_coredate_online");
        job.setJarByClass(UserCompute.class);
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        job.getConfiguration().set("type", args[2]);
        for (String path : args[0].split(",")) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(UserComputeMapper.class);
        job.setReducerClass(UserComputeReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
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
            // TODO Auto-generated method stub
            List<Option> options = new ArrayList<Option>(0);
            Option option = getParser().addHelp(
                    getParser().addStringOption("type"), "user type ");
            options.add(option);

            Option file = getParser().addHelp(
                    getParser().addStringOption("files"),
                    "confige file to resove dimention");
            options.add(file);
            return options.toArray(new Option[options.size()]);
        }

    }

}