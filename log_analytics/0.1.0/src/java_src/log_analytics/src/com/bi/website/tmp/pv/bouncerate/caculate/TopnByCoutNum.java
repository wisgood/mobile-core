/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: TopnByCoutNum.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-23 下午6:08:37 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.util.CommArgs;
import com.bi.website.tmp.pv.bouncerate.caculate.TopnByCoutNumMR.TopnByCoutNumMapper;
import com.bi.website.tmp.pv.bouncerate.caculate.TopnByCoutNumMR.TopnByCoutNumReducer;

/**
 * @ClassName: TopnByCoutNum
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-23 下午6:08:37
 */
public class TopnByCoutNum extends Configured implements Tool {

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
        CommArgs commArgs = new CommArgs();
        int nRet = 0;
        // for(int i=0;i<args.length;i++){
        //
        // System.out.println(args[i]);
        // }
        try {
            commArgs.init("topnum.jar");
            commArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            e.printStackTrace();
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(), new TopnByCoutNum(),
                commArgs.getCommsParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "TopnByCoutNumMR");
        job.setJarByClass(TopnByCoutNum.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(TopnByCoutNumMapper.class);
        job.setReducerClass(TopnByCoutNumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
