/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PlayHaltFormat.java 
 * @Package com.bi.clientquality.format 
 * @Description: 用一句话描述该文件做什么
 * @author niewf
 * @date Aug 29, 2013 8:38:35 PM 
 */
package com.bi.client.quality.format;

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

import com.bi.client.quality.format.PlayerBuffFormatMR.PlayerBuffFormatMapper;
import com.bi.client.quality.format.PlayerBuffFormatMR.PlayerBuffFormatReducer;
import com.bi.comm.paramparse.AbstractCommandParamParse;

/**
* @ClassName: PlayerBuffFormat 
* @Description: 这里用一句话描述这个类的作用 
* @author niewf
* @date Sep 2, 2013 11:08:35 PM
 */
public class PlayerBuffFormat extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        PlayerBuffFormatParamParse paramParse = new PlayerBuffFormatParamParse();
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
        nRet = ToolRunner.run(new Configuration(), new PlayerBuffFormat(),
                list.toArray(new String[list.size()]));
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();

        Job job = new Job(conf, "PlayerBuffFormatMR");
        job.setJobName("PlayerBuffFormat");

        job.setJarByClass(PlayerBuffFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PlayerBuffFormatMapper.class);
        job.setReducerClass(PlayerBuffFormatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    static class PlayerBuffFormatParamParse extends AbstractCommandParamParse {

        @Override
        public String getFunctionDescription() {
            String functionDescrtiption = "format the playerbuff log";
            return "Function :  \n    " + functionDescrtiption + "\n";
        }

        @Override
        public String getFunctionUsage() {
            // TODO Auto-generated method stub
            String functionUsage = "hadoop jar playerbuff.jar ";
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