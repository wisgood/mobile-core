package com.bi.dingzi.format;

/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FSActionConditionMR.java 
 * @Package com.bi.comm.calculate.condition 
 * @Description: 鐢ㄤ竴鍙ヨ瘽鎻忚堪璇ユ枃浠跺仛浠?箞
 * @author fuys
 * @date 2013-5-30 涓嬪崍2:30:07 
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.bi.dingzi.logenum.FsPlatformActionEnum;

/**
 * @ClassName: FSActionConditionMR
 * @Description: 杩欓噷鐢ㄤ竴鍙ヨ瘽鎻忚堪杩欎釜绫荤殑浣滅敤
 * @author fuys
 * @date 2013-5-30 涓嬪崍2:30:07
 */
public class FSActionConditionMRUTL {

    /**
     * 
     * @ClassName: FSActionConditionMRUTLMapper
     * @Description: 杩欓噷鐢ㄤ竴鍙ヨ瘽鎻忚堪杩欎釜绫荤殑浣滅敤
     * @author fuys
     * @date 2013-5-30 涓嬪崍2:31:24
     */
    public static class FSActionConditionMRUTLMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        public static final String SCREEN_SAVER = "8.ScreenSaver";

        public static final String LOCK_SCREEN = "9.LockScreen";

        private static Logger logger = Logger
                .getLogger(FSActionConditionMRUTLMapper.class.getName());

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            try {

                String[] fields = value.toString().trim().split("\t");
                String timeStr = fields[FsPlatformActionEnum.TIME.ordinal()]
                        .trim();
                String actionStr = fields[FsPlatformActionEnum.ACTION.ordinal()]
                        .trim();

                if (FSActionConditionMRUTLMapper.SCREEN_SAVER
                        .equalsIgnoreCase(actionStr)
                        || FSActionConditionMRUTLMapper.LOCK_SCREEN
                                .equalsIgnoreCase(actionStr)) {
                    context.write(new Text(timeStr), value);
                }
            }
            catch(Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error(e.getCause(), e);
            }

        }
    }

    public static class FSActionConditionMRUTLReducer extends
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
