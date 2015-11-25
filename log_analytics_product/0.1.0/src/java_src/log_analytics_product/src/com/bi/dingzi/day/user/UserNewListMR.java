/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: UserNewListMR.java 
 * @Package com.bi.dingzi.day.user 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-30 下午3:17:40 
 */
package com.bi.dingzi.day.user;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @ClassName: UserNewListMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-30 下午3:17:40
 */
public class UserNewListMR {
    public static String HISTORY_USER = "history";

    public static String NEW_USER = "distinct_mac_list";

    public static class UserNewListMap extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            filePathStr = fileInputSplit.getPath().toUri().getPath();
            System.out.println(filePathStr);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {
            try {
                String line = value.toString();
                String field[] = line.split("\t");
                // 历史用户
                if (this.filePathStr.contains(UserNewListMR.HISTORY_USER)) {
                    context.write(new Text(field[1].toUpperCase()), new Text(
                            UserNewListMR.HISTORY_USER + line));
                }
                // 每日新用户数
                else if (this.filePathStr.contains(UserNewListMR.NEW_USER)) {
                    String mac = field[1].toUpperCase();
                    context.write(new Text(mac), new Text(
                            UserNewListMR.NEW_USER + line));
                }
                
            }
            catch(ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
        }
    }

    public static class UserNewListReduce extends
            Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String newout = null;

            boolean label = false;
            boolean exLabel = false;
            for (Text val : values) {
                String value = val.toString();
                if (value.contains(UserNewListMR.HISTORY_USER)) {
                    label = true;
                }
                else {
                    exLabel = true;
                    newout = val.toString();
                }
            }

            if (null != newout) {
                if (!label && exLabel) {
                    context.write(
                            new Text(newout.trim().substring(
                                    UserNewListMR.NEW_USER.length())),
                            NullWritable.get());
                }

            }

        }

    }
}
