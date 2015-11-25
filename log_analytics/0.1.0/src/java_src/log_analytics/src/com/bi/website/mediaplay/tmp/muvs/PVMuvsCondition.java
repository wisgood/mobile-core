/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PVMuvsCondition.java 
 * @Package com.bi.website.mediaplay.tmp.muvs 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-22 下午2:58:45 
 */
package com.bi.website.mediaplay.tmp.muvs;

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

import com.bi.website.mediaplay.tmp.muvs.PVMuvsConditionMR.PVMuvsConditionMapper;
import com.bi.website.mediaplay.tmp.muvs.PVMuvsConditionMR.PVMuvsConditionReducer;

/**
 * @ClassName: PVMuvsCondition
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-22 下午2:58:45
 */
public class PVMuvsCondition extends Configured implements Tool {

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
        for(int i =0 ;i <args.length;i++){
            
            System.out.println(args[i]);
        }
        
        PVMuvsConditionArgs pvmuvsConditionArgs = new PVMuvsConditionArgs();
        int nRet = 0;

        try {
            pvmuvsConditionArgs.init("pvmuvscondition.jar");
            pvmuvsConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            // e.printStackTrace();
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new PVMuvsCondition(),
                pvmuvsConditionArgs.getOkParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "PVMuvsConditionMR");
        job.setJarByClass(PVMuvsConditionMR.class);
        job.getConfiguration().set("curl", args[2]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PVMuvsConditionMapper.class);
        job.setReducerClass(PVMuvsConditionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(60);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
