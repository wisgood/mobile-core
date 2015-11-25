package com.bi.dingzi.format;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: Condition.java 
 * @Package com.bi.comm.calculate.condition 
 * @Description: 鐢ㄤ竴鍙ヨ瘽鎻忚堪璇ユ枃浠跺仛浠?箞
 * @author fuys
 * @date 2013-5-30 涓嬪崍3:03:42 
 */

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

import com.bi.common.init.CommArgs;
import com.bi.dingzi.format.FSActionConditionMRUTL.FSActionConditionMRUTLMapper;
import com.bi.dingzi.format.FSActionConditionMRUTL.FSActionConditionMRUTLReducer;

/**
 * @ClassName: Condition
 * @Description: 杩欓噷鐢ㄤ竴鍙ヨ瘽鎻忚堪杩欎釜绫荤殑浣滅敤
 * @author fuys
 * @date 2013-5-30 涓嬪崍3:03:42
 */
public class FsActionCondition extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "FsActionConditionByColMRUTL");
        job.setJarByClass(FsActionCondition.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FSActionConditionMRUTLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FSActionConditionMRUTLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(8);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {

        CommArgs commArgs = new CommArgs();
        int nRet = 0;

        try {
            commArgs.init("fscondition.jar");
            commArgs.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }
        nRet = ToolRunner.run(new Configuration(), new FsActionCondition(),
                commArgs.getCommsParam());
        System.out.println(nRet);

    }

}
