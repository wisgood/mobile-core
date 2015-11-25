package com.bi.dingzi;

import jargs.gnu.CmdLineParser.Option;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.paramparse.AbstractCommandParamParse;
import com.bi.dingzi.DingZiMR.DingZiMapper;
import com.bi.dingzi.DingZiMR.DingZiReducer;

public class DingZi extends Configured implements Tool {
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
        DingZiParamParse paramParse = new DingZiParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        String[] params = paramParse.getParams();
        nRet = ToolRunner.run(new Configuration(), new DingZi(), params);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("indextype", args[2]);

        Job job = new Job(conf, "DingZi");
        job.setJobName("DingZi-Temp");

        job.setJarByClass(DingZi.class);
        String[] paths = args[0].split(",");
        for (String path : paths) {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DingZiMapper.class);
        job.setReducerClass(DingZiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    static class DingZiParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "format the bootstrap log";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar bootstrapformat.jar ";
            return "Usage :  \n    " + functionUsage + "\n";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            List<Option> options = new ArrayList<Option>(0);
            Option option = getParser().addHelp(
                    getParser().addStringOption("indextype"),
                    "index type,for 0");
            options.add(option);
            return options.toArray(new Option[options.size()]);
        }

    }
}
