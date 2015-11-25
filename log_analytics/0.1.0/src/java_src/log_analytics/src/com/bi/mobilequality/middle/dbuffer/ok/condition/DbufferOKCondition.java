/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DbufferOKCondition.java 
 * @Package com.bi.mobilequality.middle.dbuffer.ok.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-13 下午2:10:18 
 */
package com.bi.mobilequality.middle.dbuffer.ok.condition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobilequality.middle.dbuffer.ok.condition.DbufferOKConditionMR.DbufferOKConditionMapper;
import com.bi.mobilequality.middle.dbuffer.ok.condition.DbufferOKConditionMR.DbufferOKConditionReduce;



/**
 * @ClassName: DbufferOKCondition
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-13 下午2:10:18
 */
public class DbufferOKCondition extends Configured implements Tool {

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
        DbufferOKConditionArgs dbufferOkConditionArgs = new DbufferOKConditionArgs();
        int nRet = 0;

        try {
            dbufferOkConditionArgs.init("dbufferokcondition.jar");
            dbufferOkConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            // e.printStackTrace();
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new DbufferOKCondition(),
                dbufferOkConditionArgs.getOkParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DbufferOKConditionMR");
        job.setJarByClass(DbufferOKConditionMR.class);
        job.getConfiguration().set("ok", args[2]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DbufferOKConditionMapper.class);
        job.setReducerClass(DbufferOKConditionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
