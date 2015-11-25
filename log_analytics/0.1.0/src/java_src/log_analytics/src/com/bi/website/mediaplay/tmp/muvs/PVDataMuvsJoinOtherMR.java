package com.bi.website.mediaplay.tmp.muvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.website.videoplay.format.constant.WatchVVWebEnum;

public class PVDataMuvsJoinOtherMR {

    public static String PV_FCK_TIME = "pv_fck_time";

    public static String MINISTITELAND_FCK_TIME_TITLE = "minisiteland_fck_time_title";

    public static String FSPLAY_AFTER = "fsplay_after";

    public static class PVDataMuvsJoinOtherMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileInputSplit.getPath().toUri().getPath();
        }

        private void outPutMapFckTime(String orgStr, String enumClassStr,
                Context context) throws IOException, InterruptedException,
                ClassNotFoundException {
            String[] fields = orgStr.split(",");
            Class<Enum> logEnum = (Class<Enum>) Class.forName(enumClassStr);
            if (fields.length > Enum.valueOf(logEnum, "FCK").ordinal()) {
                context.write(new Text(fields[Enum.valueOf(logEnum, "FCK")
                        .ordinal()]),
                        new Text(fields[Enum.valueOf(logEnum, "TIMESTAMP")
                                .ordinal()]));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            try {
                String orgdataStr = value.toString();
                if (this.filePathStr
                        .contains(PVDataMuvsJoinOtherMR.PV_FCK_TIME)||this.filePathStr
                        .contains(PVDataMuvsJoinOtherMR.MINISTITELAND_FCK_TIME_TITLE)) {
                    String[] fields = orgdataStr.split("\t");
                    context.write(
                            new Text(fields[PvFCKTimeRefEnum.FCK.ordinal()]),
                            new Text(PVDataMuvsJoinOtherMR.PV_FCK_TIME
                                    + fields[PvFCKTimeRefEnum.TIMESTAMP
                                            .ordinal()]));
                }
                
                else if (this.filePathStr
                        .contains(PVDataMuvsJoinOtherMR.FSPLAY_AFTER)) {

                    this.outPutMapFckTime(orgdataStr,
                            FsplayAfterEnum.class.getName(), context);
                }
                else {
                    this.outPutMapFckTime(orgdataStr,
                            WatchVVWebEnum.class.getName(), context);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PVDataMuvsJoinOtherReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            long fckTimeStamp = 0;
            List<Long> otherTimestamplist = new ArrayList<Long>();
            for (Text value : values) {

                try {
                    String valueStr = value.toString();
                    if (valueStr.contains(PVDataMuvsJoinOtherMR.PV_FCK_TIME)) {
                        fckTimeStamp = Long.parseLong(valueStr
                                .substring(PVDataMuvsJoinOtherMR.PV_FCK_TIME
                                        .length()));

                    }
                    else {
                        otherTimestamplist.add(new Long(valueStr));
                    }
                }
                catch(NumberFormatException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
            int count = 0;
            for (int i = 0; i < otherTimestamplist.size(); i++) {

                if ((0 != fckTimeStamp)
                        && (fckTimeStamp <= otherTimestamplist.get(i)
                                .longValue())) {
                    count++;
                }
            }
            if (count > 0) {
                context.write(key, new IntWritable(count));
            }
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
