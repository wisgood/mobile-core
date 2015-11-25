/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterJoinNetworkFlowMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-27 下午2:10:18 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.bi.comm.util.MACFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;

/**
 * @ClassName: FsPlayAfterJoinNetworkFlowMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-27 下午2:10:18
 */
public class FsPlayAfterJoinNetworkFlowMR {

    public static String SUM_NETWORKFLOW = "networksum";

    public static String SUM_TOTAL = "fsplayafter/total";

    public static class FsPlayAfterJoinNetworkFlowMapper extends
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
            String macCodeKey = null;
            StringBuilder dataSB = new StringBuilder();
            String dataStr = value.toString();
            String[] fields = dataStr.split("\t");
            if (this.filePathStr
                    .contains(FsPlayAfterJoinNetworkFlowMR.SUM_NETWORKFLOW)) {
                if (fields.length > 1) {
                    macCodeKey = fields[0];
                    dataSB.append(FsPlayAfterJoinNetworkFlowMR.SUM_NETWORKFLOW);
                    dataSB.append(fields[1]);
                }
            }
            else if (this.filePathStr
                    .contains(FsPlayAfterJoinNetworkFlowMR.SUM_TOTAL)) {
                if (fields.length > 3) {
                    macCodeKey = MACFormatUtil.macFormat(fields[0]).get(
                            ConstantEnum.MAC_LONG);
                    dataSB.append(fields[0]);
                    dataSB.append("\t");
                    dataSB.append(fields[1]);
                    dataSB.append("\t");
                    dataSB.append(fields[2]);
                    dataSB.append("\t");
                    dataSB.append(fields[3]);
                }

            }
            if (null != macCodeKey && !macCodeKey.equalsIgnoreCase("")) {
                context.write(new Text(macCodeKey), new Text(dataSB.toString()
                        .trim()));
            }

        }

    }

    public static class FsPlayAfterJoinNetworkFlowReducer extends
            Reducer<Text, Text, Text, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] valueStrs = new String[2];
            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr
                        .contains(FsPlayAfterJoinNetworkFlowMR.SUM_NETWORKFLOW)) {
                    valueStrs[0] = valueStr
                            .substring(FsPlayAfterJoinNetworkFlowMR.SUM_NETWORKFLOW
                                    .length());
                }
                else {
                    valueStrs[1] = valueStr;
                }

            }
            if (null != valueStrs[0] && null != valueStrs[1]) {

                context.write(new Text(valueStrs[1] + "\t" + valueStrs[0]),
                        NullWritable.get());

            }

        }
    }

}
