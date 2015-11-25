package com.bi.dingzi.format;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.bi.common.init.ConstantEnum;
import com.bi.common.util.StringDateFormatUtils;
import com.bi.common.util.StringFormatUtils;
import com.bi.common.util.TimestampFormatUtil;

public class ExtractByColMRUTL {
    /**
     * map : According to the given column to mapping
     */

    public static class ExtractMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger.getLogger(ExtractMapper.class
                .getName());

        private String[] colNum = null;

        private String orderByColum = null;

        private String delim = null;

        public void setup(Context context) {
            colNum = context.getConfiguration().get("column").split(",");
            orderByColum = context.getConfiguration().get("orderbycolum");
            this.delim = context.getConfiguration().get("delim");
            logger.info("delim:" + this.delim);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                String[] outVal = new String[colNum.length];
                String[] field = value.toString().trim().split(this.delim);
                for (int i = 0; i < colNum.length; i++) {
                    // outVal[i] = this.getMACValue(i, field);
                    outVal[i] = field[Integer.parseInt(colNum[i])];
                }
                String orderByColumValue = field[Integer.parseInt(orderByColum)];
                logger.info(StringFormatUtils.arrayToString(outVal, "\t"));
                context.write(new Text(orderByColumValue), new Text(
                        StringFormatUtils.arrayToString(outVal, "\t")));
            }
            catch(NumberFormatException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }

    }

    public static class ExtractReduce extends
            Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                StringBuilder valueSb = new StringBuilder();
                String valueStr = val.toString();
                String[] fields = valueStr.split("\t");
                if (fields.length == 2) {
                    valueSb.append(TimestampFormatUtil.formatTimestamp(
                            fields[0]).get(ConstantEnum.DATE_ID));
                    valueSb.append("\t");
                    valueSb.append(fields[1]);
                }
                else if (fields.length == 1) {
                    valueSb.append(TimestampFormatUtil.formatTimestamp(
                            fields[0]).get(ConstantEnum.DATE_ID));
                    valueSb.append("\t");
                    valueSb.append("");

                }
                context.write(new Text(valueSb.toString()), NullWritable.get());
            }

        }

    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // String bootExtractColumn = "0,1,2,3,4,8,6,10";
        // String bootOut = "output_bootsrap";
        // String bootTmpOut = "output_extract_com_boot";
        // String bootOrderByCol = "10";
        // String exitExtractColumn = "0,1,2,3,4,5,6,9";
        // String exitput = "output_exit";
        // String exitTmpOut = "output_extract_com_exit";
        // String exitOrderByCol = "9";

        String historyUserColumn = "6,4,2";
        String historyUserInput = "output_distinct_comdm";
        String historyUserOut = "output_hist_user";
        String historyUserOrderByCol = "7";
        Job job = new Job();

        // job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
        // job.setInputFormatClass(TextInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(ExtractByCol.class);
        job.setJobName("ExtractMRUTL");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        Configuration conf = job.getConfiguration();
        // conf.set("column", bootExtractColumn);
        // conf.set("orderbycolum", bootOrderByCol);
        // FileInputFormat.setInputPaths(job, new Path(bootOut));
        // FileOutputFormat.setOutputPath(job, new Path(bootTmpOut));
        // conf.set("column", exitExtractColumn);
        // conf.set("orderbycolum", exitOrderByCol);
        // FileInputFormat.setInputPaths(job, new Path(exitput));
        // FileOutputFormat.setOutputPath(job, new Path(exitTmpOut));

        conf.set("column", historyUserColumn);
        FileInputFormat.setInputPaths(job, new Path(historyUserInput));
        FileOutputFormat.setOutputPath(job, new Path(historyUserOut));
        conf.set("orderbycolum", historyUserOrderByCol);
        job.setMapperClass(ExtractMapper.class);
        job.setReducerClass(ExtractReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
