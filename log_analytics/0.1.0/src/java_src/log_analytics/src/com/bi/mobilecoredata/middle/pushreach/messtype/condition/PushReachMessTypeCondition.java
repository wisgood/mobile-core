package com.bi.mobilecoredata.middle.pushreach.messtype.condition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.mobilecoredata.middle.pushreach.messtype.condition.PushReachMessTypeConditionMR.PushReachMessTypeConditionMapper;
import com.bi.mobilecoredata.middle.pushreach.messtype.condition.PushReachMessTypeConditionMR.PushReachMessTypeConditionReduce;

public class PushReachMessTypeCondition extends Configured implements Tool {

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
        PushReachMessTypeConditionArgs pushReachMessTypeConditionArgs = new PushReachMessTypeConditionArgs();
        int nRet = 0;

        try {
            pushReachMessTypeConditionArgs
                    .init("pushReachMessTypeCondition.jar");
            pushReachMessTypeConditionArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            // countArgs.parser.printUsage();
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(),
                new PushReachMessTypeCondition(),
                pushReachMessTypeConditionArgs.getConditionssParam());
        System.out.println(nRet);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub

        Configuration conf = getConf();
        Job job = new Job(conf, "PushReachMessTypeConditionMR");
        job.setJarByClass(PushReachMessTypeConditionMR.class);
        job.getConfiguration().set("conditions", args[2]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PushReachMessTypeConditionMapper.class);
        job.setReducerClass(PushReachMessTypeConditionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}
