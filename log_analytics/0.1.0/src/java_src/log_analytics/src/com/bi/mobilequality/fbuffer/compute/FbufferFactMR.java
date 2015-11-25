package com.bi.mobilequality.fbuffer.compute;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobilequality.fbuffer.format.dataenum.FbufferQualityFormatEnum;

public class FbufferFactMR {

    public static class FbufferFactMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private String[] idColumns;

        public void setup(Context context) throws NumberFormatException {

            try {
                idColumns = context.getConfiguration().get("idcolumns")
                        .split(",");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException,
                UnsupportedEncodingException {
            String[] fields = value.toString().split("\t");

            //
//            String okType = fields[FbufferFormatEnum.OK.ordinal()];
//            int btmFormat = Integer
//                    .parseInt(fields[FbufferFormatEnum.BTM_FORMAT.ordinal()]);
//            int bposFormat = Integer
//                    .parseInt(fields[FbufferFormatEnum.BPOS_FORMAT.ordinal()]);
            
          String okType = fields[2];
          int btmFormat = Integer
                  .parseInt(fields[1]);
          int bposFormat = Integer
                  .parseInt(fields[3]);
            int success = 0;
            int fail = 0;
            int successFromZero = 0;
            int failFromZero = 0;
            int tagFromZero = 0;
            if (bufferOK(okType)) {
                success = 1;
                fail = 0;
                if (bposFormat == 0) {
                    tagFromZero = 1;
                    successFromZero = 1;
                    failFromZero = 0;
                }
            }
            else {
                success = 0;
                fail = 1;
                btmFormat = 0;
                if (bposFormat == 0) {
                    tagFromZero = 1;
                    successFromZero = 0;
                    failFromZero = 1;
                    bposFormat = 0;
                }
            }

            StringBuilder mapOutputKey = new StringBuilder();
            for (int i = 0; i < idColumns.length; i++) {
                if (i != idColumns.length - 1) {
                    mapOutputKey.append(fields[Integer.parseInt(idColumns[i])]
                            + "\t");
                }
                else {
                    mapOutputKey.append(fields[Integer.parseInt(idColumns[i])]);
                }

            }
            Text outputKey = new Text(mapOutputKey.toString());
            StringBuffer mapOutputValue = new StringBuffer();
            mapOutputValue.append(btmFormat);
            mapOutputValue.append("\t");
            mapOutputValue.append(success);
            mapOutputValue.append("\t");
            mapOutputValue.append(fail);
            mapOutputValue.append("\t");
            mapOutputValue.append(tagFromZero);
            mapOutputValue.append("\t");
            mapOutputValue.append(successFromZero);
            mapOutputValue.append("\t");
            mapOutputValue.append(failFromZero);
            Text outputValue = new Text(mapOutputValue.toString());
            context.write(outputKey, outputValue);

        }

        private boolean bufferOK(String okType) {
            if (null == okType)
                return false;
            try {
                return Integer.parseInt(okType) == 0 ? true : false;

            }
            catch(Exception e) {
                return false;
            }
        }

    }

    public static class FbufferFactReducer extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long successSum = 0;
            long failSum = 0;
            long successFromZeroSum = 0;
            long failFromZeroSum = 0;
            long bTimeSum = 0;
            long bTimeSumFromZero = 0;
            for (Text value : values) {
                String[] field = value.toString().split("\t");
                long bTime = Integer.parseInt(field[0]);
                long success = Integer.parseInt(field[1]);
                long fail = Integer.parseInt(field[2]);
                long tagFromZero = Integer.parseInt(field[3]);
                long successFromZero = Integer.parseInt(field[4]);
                long failFromZero = Integer.parseInt(field[5]);
                successSum += success;
                failSum += fail;
                bTimeSum += bTime;
                if (tagFromZero == 1) {
                    successFromZeroSum += successFromZero;
                    failFromZeroSum += failFromZero;
                    bTimeSumFromZero += bTime;

                }

            }
            StringBuilder value = new StringBuilder();
            // btime,successsum,failsum

            value.append(bTimeSum);
            value.append("\t");
            value.append(successSum);
            value.append("\t");
            value.append(successSum + failSum);
            value.append("\t");

            // btimefromzero,successsumfromzero,failsumfromzero

            value.append(bTimeSumFromZero);
            value.append("\t");
            value.append(successFromZeroSum);
            value.append("\t");
            value.append(successFromZeroSum + failFromZeroSum);
            value.append("\t");

            // btimenofromzero,successsumnofromzero,failsumnofromzero

            value.append(bTimeSum - bTimeSumFromZero);
            value.append("\t");
            value.append(successSum - successFromZeroSum);
            value.append("\t");
            value.append(successSum + failSum - successFromZeroSum
                    - failFromZeroSum);

            context.write(key, new Text(value.toString()));

        }

    }

    /**
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub

        Job job = new Job();
        job.setJarByClass(FbufferFactMR.class);
        job.setJobName("FbufferFact-MobileQuality");
        job.getConfiguration().set("mapred.job.tracker", "local");
        job.getConfiguration().set("fs.default.name", "local");
        // 设置配置文件默认路径
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_PLATY_FILEPATH.name(),
                "conf/dm_mobile_platy");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_QUDAO_FILEPATH.name(),
                "conf/dm_mobile_qudao");
        job.getConfiguration().set(ConstantEnum.IPTABLE_FILEPATH.name(),
                "conf/ip_table");
        job.getConfiguration().set(
                ConstantEnum.DM_COMMON_INFOHASH_FILEPATH.name(),
                "conf/dm_common_infohash");
        job.getConfiguration().set(
                ConstantEnum.DM_MOBILE_SERVER_FILEPATH.name(),
                "conf/dm_mobile_server");
        FileInputFormat.addInputPath(job, new Path("mq_output_fbuffer"));
        // FileInputFormat.addInputPath(job, new Path("input_fbuffer_fact"));
        FileOutputFormat.setOutputPath(job,
                new Path("mq_output_fbuffer_fact11"));
        job.setMapperClass(FbufferFactMapper.class);
        job.setReducerClass(FbufferFactReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
