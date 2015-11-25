/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: MinisiteLandFCKTimeURLMR.java 
 * @Package com.bi.website.mediaplay.tmp.muvs 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-3 上午11:06:34 
 */
package com.bi.website.mediaplay.tmp.muvs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import com.bi.comm.util.StringDecodeFormatUtil;

/**
 * @ClassName: MinisiteLandFCKTimeURLMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-3 上午11:06:34
 */
public class MinisiteLandFCKTimeURLMR {

    public static class MinisiteLandFCKTimeURLMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orginData = value.toString();
            String[] fields = orginData.split(",");
            if (fields.length > MinisitelandEnum.FCK.ordinal()) {
                String userFCK = fields[MinisitelandEnum.FCK.ordinal()];
                String timestampStr = StringDecodeFormatUtil.decodeCodedStr(
                        fields[MinisitelandEnum.TIMESTAMP.ordinal()], "UTF-8");
                String title = fields[MinisitelandEnum.TITLE.ordinal()];
                StringBuilder valueSB = new StringBuilder();
                valueSB.append(userFCK);
                valueSB.append("\t");
                valueSB.append(timestampStr);
                valueSB.append("\t");
                valueSB.append(title);
                context.write(new Text(userFCK), new Text(valueSB.toString()));
            }

        }

    }

    public static class MinisiteLandFCKTimeURLReducer extends
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
