package com.bi.comm.calculate.bufferindex;

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

import com.bi.comm.calculate.bufferindex.BufferIndexMR.BufferIndexMapper;
import com.bi.comm.calculate.bufferindex.BufferIndexMR.BufferIndexReducer;
import com.bi.comm.paramparse.AbstractCommandParamParse;

public class BufferIndex extends Configured implements Tool {

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
        BufferIndexCommandParamParse paramParse = new BufferIndexCommandParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        String[] params = paramParse.getParams();
        nRet = ToolRunner.run(new Configuration(), new BufferIndex(), params);
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.set("idcolumns", args[2]);
        conf.set("indexcolumns", args[3]);
        Job job = new Job(conf, "BufferIndex_MobileServiceQuality");
        job.setJarByClass(BufferIndex.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BufferIndexMapper.class);
        job.setReducerClass(BufferIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static class BufferIndexCommandParamParse extends
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

            Option indexColumnOption = getParser().addHelp(
                    getParser().addStringOption("indexcolumns"),
                    "the index column");
            options.add(indexColumnOption);
            return options.toArray(new Option[options.size()]);
        }

    }
}
