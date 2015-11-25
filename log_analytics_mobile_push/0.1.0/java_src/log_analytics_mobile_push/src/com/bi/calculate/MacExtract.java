package com.bi.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;

import com.bi.common.util.StringUtil;

public class MacExtract extends Configured implements Tool {

    private static final String SEPARATOR = "\t";

    public static class MacExtractMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private String filePath;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath = fileSplit.getPath().getParent().toString();

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = StringUtil.splitLog(line, '\t');
            if (fromBootstrap(filePath)) {

            }
            else if (fromDownload(filePath) || fromFbuffer(filePath)) {
            }

        }

        private boolean fromBootstrap(String filePath) {
            return filePath.toLowerCase().contains("bootstrap".toLowerCase());
        }

        private boolean fromFbuffer(String filePath) {
            return filePath.toLowerCase().contains("fbuffer".toLowerCase());
        }

        private boolean fromDownload(String filePath) {
            return filePath.toLowerCase().contains("download".toLowerCase());
        }

    }

    public static class MacExtractReducer extends
            Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

        }

    }

    /**
     * @throws Exception
     * 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

}
