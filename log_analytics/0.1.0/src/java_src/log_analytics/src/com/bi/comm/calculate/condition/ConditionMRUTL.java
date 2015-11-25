/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: ConditionMR.java 
 * @Package com.bi.comm.calculate.condition 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-30 下午2:30:07 
 */
package com.bi.comm.calculate.condition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * @ClassName: ConditionMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-30 下午2:30:07
 */
public class ConditionMRUTL {

    /**
     * 
     * @ClassName: ConditionMRUTLMapper
     * @Description: 这里用一句话描述这个类的作用
     * @author fuys
     * @date 2013-5-30 下午2:31:24
     */
    public static class ConditionMRUTLMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private static Logger logger = Logger
                .getLogger(ConditionMRUTLMapper.class.getName());

        // groupby 列数
        private String[] colNum = null;

        // 需要判断条件的列数
        private String[] conditionColNums = null;

        // 需要判断条件值 (不等于前@号)
        private String[] conditionValues = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.colNum = context.getConfiguration().get("groupby").split(",");
            this.conditionColNums = context.getConfiguration()
                    .get("conditioncol").split(",");
            this.conditionValues = context.getConfiguration()
                    .get("conditionvalue").split(",");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            try {
                StringBuilder colKeySb = new StringBuilder();
                String[] field = value.toString().trim().split("\t");
                for (int i = 0; i < colNum.length; i++) {
                    colKeySb.append(field[Integer.parseInt(colNum[i])]);
                    if (i < colNum.length - 1) {
                        colKeySb.append("\t");
                    }
                }
                logger.info(this.conditionColNums.length);
                if (this.conditionColNums.length == 1) {

                    String colConditionStr = field[Integer
                            .parseInt(this.conditionColNums[0])];
                    logger.info(this.conditionColNums[0]);
                    logger.info(this.conditionValues[0]);
                    if (!this.conditionValues[0].contains("@")) {
                        logger.info("*************************************************************************");
                        logger.info(colConditionStr);
                        logger.info(this.conditionValues[0].trim());
                        logger.info(colConditionStr
                                .equalsIgnoreCase(this.conditionValues[0]
                                        .trim()));
                        logger.info("*************************************************************************");
                        if (colConditionStr
                                .equalsIgnoreCase(this.conditionValues[0]
                                        .trim())) {
                            context.write(new Text(colKeySb.toString()), value);
                        }
                    }
                    else if (this.conditionValues[0].contains("or")) {
                        String[] splitValues = this.conditionValues[0]
                                .split("or");

                        if (splitValues.length == 2) {
                            if (colConditionStr
                                    .equalsIgnoreCase(splitValues[0])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[1])) {
                                context.write(new Text(colKeySb.toString()),
                                        value);
                            }
                        }
                        else if (splitValues.length == 3) {
                            if (colConditionStr
                                    .equalsIgnoreCase(splitValues[0])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[1])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[2])) {
                                context.write(new Text(colKeySb.toString()),
                                        value);
                            }
                        }
                        else if (splitValues.length == 4) {
                            if (colConditionStr
                                    .equalsIgnoreCase(splitValues[0])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[1])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[2])
                                    || colConditionStr
                                            .equalsIgnoreCase(splitValues[3])) {
                                context.write(new Text(colKeySb.toString()),
                                        value);
                            }
                        }

                    }
                    else {

                        String conditionValue = this.conditionValues[0]
                                .substring(1);
                        if (!(colConditionStr.equalsIgnoreCase(conditionValue
                                .trim()))) {

                            context.write(new Text(colKeySb.toString()), value);
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

                            context.write(new Text(colKeySb.toString()), value);
                        }
                    }
                    else {

                        String conditionValue2 = this.conditionValues[1]
                                .substring(1);
                        if (colConditionStr1
                                .equalsIgnoreCase(this.conditionValues[0])
                                && !(colConditionStr2
                                        .equalsIgnoreCase(conditionValue2))) {

                            context.write(new Text(colKeySb.toString()), value);
                        }

                    }

                }

            }
            catch(Exception e) {
                logger.error(e.getMessage(), e);
            }

        }

    }

    public static class ConditionMRUTLReducer extends
            Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            for (Text value : values) {

                context.write(value, NullWritable.get());
            }
        }

    }
}
