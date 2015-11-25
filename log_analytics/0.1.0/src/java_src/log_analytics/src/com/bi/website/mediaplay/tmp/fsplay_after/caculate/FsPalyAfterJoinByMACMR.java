/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPalyAfterJoinByMACMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-22 上午10:54:28 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.bi.comm.util.MACFormatUtil;

/**
 * @ClassName: FsPalyAfterJoinByMACMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-22 上午10:54:28
 */
public class FsPalyAfterJoinByMACMR {
    public static final String SUM_COUNT = "sumcount";

    public static final String SUM_DISCOUNT = "sumdiscount";

    public static final String LAST_DATE = "lastdate";

    public static class FsPalyAfterJoinByMACMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub

            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            filePathStr = fileInputSplit.getPath().toUri().getPath();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            try {
               

                StringBuilder dataSB = new StringBuilder();

                if (filePathStr.contains(FsPalyAfterJoinByMACMR.SUM_COUNT)) {
                    dataSB.append(FsPalyAfterJoinByMACMR.SUM_COUNT);

                }
                else if (filePathStr
                        .contains(FsPalyAfterJoinByMACMR.SUM_DISCOUNT)) {
                    dataSB.append(FsPalyAfterJoinByMACMR.SUM_DISCOUNT);
                }
                else {
                    dataSB.append(FsPalyAfterJoinByMACMR.LAST_DATE);
                }
                String dataStr = value.toString();
                String[] dataStrs = dataStr.split("\t");
                if (dataStrs.length == 3) {
                    dataSB.append(dataStrs[1]);
                    dataSB.append("\t");
                    dataSB.append(dataStrs[2]);
                }
                else if (dataStrs.length == 2) {
                    dataSB.append(dataStrs[1]);

                }
                String macStr = dataStrs[0];
                MACFormatUtil.isCorrectMac(macStr);
                context.write(new Text(dataStrs[0]),
                        new Text(dataSB.toString()));
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public static class FsPalyAfterJoinByMACReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String[] valueStrs = new String[3];
            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.contains(FsPalyAfterJoinByMACMR.SUM_COUNT)) {
                    valueStrs[0] = valueStr
                            .substring(FsPalyAfterJoinByMACMR.SUM_COUNT
                                    .length());
                    // listValue.add(0, valueStr
                    // .substring(FsPalyAfterJoinByMACMR.SUM_COUNT
                    // .length()));
                }
                else if (valueStr.contains(FsPalyAfterJoinByMACMR.SUM_DISCOUNT)) {
                    valueStrs[1] = valueStr
                            .substring(FsPalyAfterJoinByMACMR.SUM_DISCOUNT
                                    .length());
                    // listValue.add(1, valueStr
                    // .substring(FsPalyAfterJoinByMACMR.SUM_DISCOUNT
                    // .length()));
                }
                else {
                    valueStrs[2] = valueStr
                            .substring(FsPalyAfterJoinByMACMR.LAST_DATE
                                    .length());
                    // listValue.add(2, valueStr
                    // .substring(FsPalyAfterJoinByMACMR.LAST_DATE
                    // .length()));
                }

            }
            context.write(key, new Text(valueStrs[0] + "\t" + valueStrs[1]
                    + "\t" + valueStrs[2]));

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
