package com.bi.website.mediaplay.tmp.muvs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.bi.comm.util.StringDecodeFormatUtil;

public class PVMuvsConditionErrorMR {

    public static class PVMuvsConditionMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String conditionUrl = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.conditionUrl = context.getConfiguration().get("curl");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orginData = value.toString();
            String[] fields = orginData.split(",");
            if (fields.length > PVDataEnum.FCK.ordinal()) {
                String userFCK = fields[PVDataEnum.FCK.ordinal()];
                String timestampStr = fields[PVDataEnum.TIMESTAMP.ordinal()];
                String refferURL = StringDecodeFormatUtil.decodeCodedStr(
                        fields[PVDataEnum.REFERRER.ordinal()], "UTF-8");
                if (!("".equalsIgnoreCase(refferURL))
                        && (refferURL.contains(this.conditionUrl))) {
                 
                }else {
                    
                    
                }

            }else {
                
            }

        }

    }

    public static class PVMuvsConditionReducer extends
            Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Long, Text> timeTree = new TreeMap<Long, Text>();
            for (Text value : values) {
                String valueStr = value.toString();
                String[] valueStrs = valueStr.split("\t");
                timeTree.put(new Long(valueStrs[1]), value);

            }
            context.write(NullWritable.get(), timeTree.firstEntry().getValue());
        }

    }

    /**
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
