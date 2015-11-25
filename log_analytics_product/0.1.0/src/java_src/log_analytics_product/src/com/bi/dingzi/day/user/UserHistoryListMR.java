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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @ClassName: UserNewListMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-30 下午3:17:40
 */
public class UserHistoryListMR {
    public static String HISTORY_USER = "history";

    public static String NEW_USER = "distinct_mac_list";

    public static class UserHistoryListMap extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        private String dateStr = null;

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
                String line = value.toString().trim();
                String field[] = line.split("\t");
                // 历史用户
                if (this.filePathStr.contains(UserHistoryListMR.HISTORY_USER)) {
                    if (null == this.dateStr) {
                        try {
                            SimpleDateFormat df = new SimpleDateFormat(
                                    "yyyyMMdd");
                            Date d = df.parse(field[0]);
                            this.dateStr = df.format(new Date(d.getTime() + 1
                                    * 24 * 60 * 60 * 1000));
                        }
                        catch(ParseException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                    context.write(new Text(field[1].toUpperCase()), new Text(
                            UserHistoryListMR.HISTORY_USER + dateStr));
                }
                // 每日用户数
                else if (this.filePathStr.contains(UserHistoryListMR.NEW_USER)) {
                    String mac = field[1].toUpperCase();
                    context.write(new Text(mac), new Text(
                            UserHistoryListMR.NEW_USER + field[0]));
                }
            }
            catch(ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
        }
    }

    public static class UserHistoryListReduce extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String valueStr = null;
            boolean label = false;
            boolean exLabel = false;
            for (Text val : values) {
                valueStr = val.toString();
                if (valueStr.contains(UserHistoryListMR.HISTORY_USER)) {
                    label = true;
                }
                else {
                    exLabel = true;

                }
            }
            if (null != valueStr) {
                if (label) {
                   

                    if (valueStr.trim().length() == UserHistoryListMR.NEW_USER
                            .length() + 8) {
                        context.write(

                                new Text(valueStr.trim().substring(
                                        UserHistoryListMR.NEW_USER.length())),
                                key);

                    }
                    if (valueStr.trim().length() == UserHistoryListMR.HISTORY_USER
                            .length() + 8) {
                        context.write(

                                new Text(valueStr.trim()
                                        .substring(
                                                UserHistoryListMR.HISTORY_USER
                                                        .length())), key);

                    }

                }

                if (!label && exLabel) {
                    context.write(

                            new Text(valueStr.trim().substring(
                                    UserHistoryListMR.NEW_USER.length())), key);
                }
            }

        }
    }
}
