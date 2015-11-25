package com.bi.mobilequality.fbuffer.compute;

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

import com.bi.comm.paramparse.AbstractCommandParamParse;
import com.bi.mobilequality.fbuffer.compute.FbufferFactMR.FbufferFactMapper;
import com.bi.mobilequality.fbuffer.compute.FbufferFactMR.FbufferFactReducer;


public class FbufferFact extends Configured implements Tool{

    /**
     * @throws Exception  
     *
     * @Title: main 
     * @Description: 这里用一句话描述这个方法的作用 
     * @param   @param args 参数说明
     * @return void    返回类型说明 
     * @throws 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        FbufferCommandParamParse paramParse = new FbufferCommandParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        String[] params = paramParse.getParams();
        nRet = ToolRunner.run(new Configuration(), new FbufferFact(), params);
        System.out.println(nRet);


    }

    public static class FbufferCommandParamParse extends
            AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "compute the buffer index";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar bufferindex.jar ";
            return "Usage :  \n    " + functionUsage + "\n";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            List<Option> options = new ArrayList<Option>(0);
            Option idColumnOption = getParser().addHelp(
                    getParser().addStringOption("idcolumns"),
                    "the group column");
            options.add(idColumnOption);
            return options.toArray(new Option[options.size()]);
        }

    }




    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("idcolumns", args[2]);
        Job job = new Job(conf, "FbufferFact_MobileServiceQuality");

        job.setJarByClass(FbufferFact.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FbufferFactMapper.class);
        job.setReducerClass(FbufferFactReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
