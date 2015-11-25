package com.bi.mobilecoredata.middle.bootstrap;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bi.mobile.bootstrap.format.dataenum.BootStrapFormatEnum;

public class BootStrapSplitMR {

    public static class BootStrapSplitMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String orgiData = value.toString();
                String[] line = orgiData.split("\t");
                String btype = line[BootStrapFormatEnum.BOOT_TYPE.ordinal()];
                if (containBtype(btype)) {
                    String okType = line[BootStrapFormatEnum.OK_TYPE.ordinal()];
                    line[BootStrapFormatEnum.OK_TYPE.ordinal()] = String
                            .valueOf(processOkType(okType));
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(line[BootStrapFormatEnum.DATE_ID
                            .ordinal()]);
                    stringBuilder.append("\t");
                    stringBuilder.append(line[BootStrapFormatEnum.PLAT_ID
                            .ordinal()]);
                    stringBuilder.append("\t");
                    stringBuilder.append(line[BootStrapFormatEnum.VERSION_STR
                            .ordinal()]);
                    stringBuilder.append("\t");
                    stringBuilder.append(line[BootStrapFormatEnum.BOOT_TYPE
                            .ordinal()]);
                    stringBuilder.append("\t");
                    stringBuilder.append(line[BootStrapFormatEnum.OK_TYPE
                            .ordinal()]);
                    context.write(new Text(stringBuilder.toString()), new Text(
                            line[BootStrapFormatEnum.MACCLEAN.ordinal()]));

                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        private boolean containBtype(String btype) {

            try {
                int type = Integer.parseInt(btype);
                if (type == 3 || type == 7)
                    return true;
                else {
                    return false;
                }

            }
            catch(Exception e) {
                return false;
            }

        }

        private int processOkType(String okType) {
            if (null == okType)
                return -1;

            try {
                int type = Integer.parseInt(okType);
                if (type == 1) {
                    return 1;

                }
                else {
                    return -1;
                }
            }
            catch(Exception e) {
                return -1;
            }
        }

    }

    public static class BootStrapSplitReducer extends
            Reducer<Text, Text, Text, Text> {
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Job job = new Job();
        job.setJarByClass(BootStrapSplitMR.class);
        job.setJobName("BootStrapSplitMR");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        FileInputFormat.setInputPaths(job, new Path("test_input"));
        FileOutputFormat.setOutputPath(job, new Path("test_output"));
        job.setMapperClass(BootStrapSplitMapper.class);
        job.setReducerClass(BootStrapSplitReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
