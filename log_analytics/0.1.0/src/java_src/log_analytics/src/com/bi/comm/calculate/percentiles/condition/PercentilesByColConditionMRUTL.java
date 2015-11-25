/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: PercentilesByColConditionConditionMRUTL.java 
 * @Package com.bi.comm.calculate.percentiles.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-15 下午5:52:02 
 */
package com.bi.comm.calculate.percentiles.condition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bi.comm.util.PercenTilesUtil;

/**
 * @ClassName: PercentilesByColConditionConditionMRUTL
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-15 下午5:52:02
 */
public class PercentilesByColConditionMRUTL {
    public static class PercentilesByColConditionMapper extends
            Mapper<LongWritable, Text, Text, LongWritable> {
        // groupby 列数
        private String[] colNum = null;

//        // 需要计算pn的列数
        private String needCaPnColNum = null;

        // 需要判断条件的列数
        private String[] conditionColNums = null;

        // 需要判断条件值 (不等于前@号)
        private String[] conditionValues = null;

        @Override
        public void setup(Context context) {
            this.colNum = context.getConfiguration().get("groupby").split(",");
            this.needCaPnColNum = context.getConfiguration().get("pncolindex");
            this.conditionColNums = context.getConfiguration()
                    .get("conditioncol").split(",");
            this.conditionValues = context.getConfiguration()
                    .get("conditionvalue").split(",");
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringBuilder colKeySb = new StringBuilder();
            String[] field = value.toString().trim().split("\t");
            for (int i = 0; i < colNum.length; i++) {
                colKeySb.append(field[Integer.parseInt(colNum[i])]);
                if (i < colNum.length - 1) {
                    colKeySb.append("\t");
                }
            }

            if (this.conditionColNums.length == 1) {

                String colConditionStr = field[Integer
                        .parseInt(this.conditionColNums[0])];

                if (!this.conditionValues[0].contains("@")) {
                    if (colConditionStr
                            .equalsIgnoreCase(this.conditionValues[0])) {
                        long needCalPn = Long.parseLong(field[Integer
                                .parseInt(this.needCaPnColNum)]);
                        context.write(new Text(colKeySb.toString()),
                                new LongWritable(needCalPn));
                    }
                }
                else {

                    String conditionValue = this.conditionValues[0]
                            .substring(1);
                    if (!(colConditionStr.equalsIgnoreCase(conditionValue))) {
                        long needCalPn = Long.parseLong(field[Integer
                                .parseInt(this.needCaPnColNum)]);
                        context.write(new Text(colKeySb.toString()),
                                new LongWritable(needCalPn));
                    }

                }
            }
            if (this.conditionColNums.length == 2) {

                String colConditionStr1 = field[Integer
                        .parseInt(this.conditionColNums[0])];
                String colConditionStr2 = field[Integer
                        .parseInt(this.conditionColNums[1])];

                if (!this.conditionValues[1].contains("@")) {
                    if (colConditionStr1
                            .equalsIgnoreCase(this.conditionValues[0])
                            && colConditionStr2
                                    .equalsIgnoreCase(this.conditionValues[1])) {
                        long needCalPn = Long.parseLong(field[Integer
                                .parseInt(this.needCaPnColNum)]);
                        context.write(new Text(colKeySb.toString()),
                                new LongWritable(needCalPn));
                    }
                }
                else {

                    String conditionValue2 = this.conditionValues[1]
                            .substring(1);
                    if (colConditionStr1
                            .equalsIgnoreCase(this.conditionValues[0])
                            && !(colConditionStr2
                                    .equalsIgnoreCase(conditionValue2))) {
                        long needCalPn = Long.parseLong(field[Integer
                                .parseInt(this.needCaPnColNum)]);
                        context.write(new Text(colKeySb.toString()),
                                new LongWritable(needCalPn));
                    }

                }

            }

        }

    }

    public static class PercentilesByColConditionReducer extends
            Reducer<Text, LongWritable, Text, Text> {
        private static Logger logger = Logger
                .getLogger(PercentilesByColConditionReducer.class.getName());

        private String[] percentStrs = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            super.setup(context);
            this.percentStrs = context.getConfiguration().get("percent").trim()
                    .split(",");

        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                Context context) throws IOException, InterruptedException {
            List<Long> longLists = new ArrayList<Long>();
            for (LongWritable value : values) {
                longLists.add(value.get());
            }
            int longListLength = longLists.size();
            long[] longArrgs = new long[longListLength];
            for (int i = 0; i < longListLength; i++) {
                Long tmpValue = longLists.get(i);
                longArrgs[i] = (tmpValue == null ? 0 : tmpValue);

            }
            if (null != this.percentStrs && 1 == this.percentStrs.length) {

                float percent = Float.parseFloat(this.percentStrs[0]);
                double percentitle = PercenTilesUtil.percenTitle(percent,
                        longArrgs);
                context.write(key, new Text(percentitle + ""));
            }

            if (null != this.percentStrs && 1 < this.percentStrs.length) {

                StringBuilder percentitlesSB = new StringBuilder();
                for (int i = 0; i < this.percentStrs.length; i++) {
                    float percent = Float.parseFloat(this.percentStrs[i]);
                    long percentitle = (long) PercenTilesUtil.percenTitle(
                            percent, longArrgs);
                    percentitlesSB.append(percentitle);
                    if (i < this.percentStrs.length - 1) {
                        percentitlesSB.append("\t");
                    }
                }
                context.write(key, new Text(percentitlesSB.toString()));
            }
            
            if(null == this.percentStrs || 0 == this.percentStrs.length){
                
                
                
                
            }

        }
    }
   
}
