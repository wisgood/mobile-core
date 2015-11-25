package com.bi.mobilequality.fbuffer.format.correctdata;

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
import com.bi.mobilequality.fbuffer.format.correctdata.FbufferQualityFormatMR.FbufferQualityFormatMapper;
import com.bi.mobilequality.fbuffer.format.correctdata.FbufferQualityFormatMR.FbufferQualityFormatReducer;


public class FbufferQualityFormat extends Configured implements Tool {

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        FbufferQualityFormatParamParse paramParse = new FbufferQualityFormatParamParse();
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
        nRet = ToolRunner.run(new Configuration(), new FbufferQualityFormat(),
                list.toArray(new String[list.size()]));
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "FbufferQualityFormat");

        job.setJarByClass(FbufferQualityFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FbufferQualityFormatMapper.class);
        job.setReducerClass(FbufferQualityFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    static class FbufferQualityFormatParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "format the fbuffer log";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar fbufferformat.jar ";
            return "Usage :  \n    " + functionUsage + "\n";
        }

        @Override
        public Option[] getOptions() {
            // TODO Auto-generated method stub
            List<Option> options = new ArrayList<Option>(0);
            Option option = getParser().addHelp(
                    getParser().addStringOption("files"),
                    "confige file to resove dimention");
            options.add(option);
            return options.toArray(new Option[options.size()]);
        }

    }

}
