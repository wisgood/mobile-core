package com.bi.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.common.logenum.FormatBootStrapEnum;
import com.bi.common.paramparse.AbstractCmdParamParse;
import com.bi.common.paramparse.BaseCmdParamParse;
import com.bi.common.paramparse.ConfigFileCmdParamParse;
import com.bi.common.util.HdfsUtil;
import com.bi.common.util.StringUtil;

public class BootstrapMacFirstExtract extends Configured implements Tool {

    private static final char SEPARATOR = '\t';

    public static class BootstrapMacFirstExtractMapper extends
            Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = StringUtil.splitLog(line, SEPARATOR);
            String mac = fields[FormatBootStrapEnum.MAC.ordinal()];
            context.write(new Text(mac), NullWritable.get());

        }

    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        AbstractCmdParamParse paramParse = new BaseCmdParamParse();
        int nRet = 0;
        try {
            paramParse.parse(args);
        }
        catch(Exception e) {
            System.out.println(e.toString());
            System.exit(1);
        }

        nRet = ToolRunner.run(new Configuration(),
                new BootstrapMacFirstExtract(), paramParse.getParams());
        System.out.println(nRet);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "pushreach-bootstrap-format");
        job.setJarByClass(BootstrapMacFirstExtract.class);
        HdfsUtil.deleteDir(args[1]);
        for (Path path : HdfsUtil.listPaths(args[0])) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BootstrapMacFirstExtractMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
