/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterMacByCityMR.java 
 * @Package com.bi.website.mediaplay.tmp.macbycity.fsplay_after 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-6 下午2:51:25 
 */
package com.bi.website.mediaplay.tmp.macbycity.fsplay_after;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.comm.util.TimestampFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * @ClassName: FsPlayAfterMacByCityMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-6 下午2:51:25
 */
public class FsPlayAfterMacByCityMR {

    public static class FsPlayAfterMacByCityMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            this.dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String valueStr = value.toString();
            String[] fields = valueStr.split(",");

            if (fields.length > FsplayAfterEnum.MAC.ordinal()) {
                try {
                    // String timeValueStr = TimestampFormatUtil
                    // .formatTimeStamp(fields[FsplayAfterEnum.TIMESTAMP
                    // .ordinal()]);
                    long ipLong = 0;
                    ipLong = IPFormatUtil.ip2long(fields[FsplayAfterEnum.IP
                            .ordinal()]);
                    java.util.Map<ConstantEnum, String> ipRuleMap;
                    ipRuleMap = dmIPRuleDAO.getDMOjb(ipLong);
                    String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                    context.write(new Text(
                            fields[FsplayAfterEnum.MAC.ordinal()]), new Text(
                            fields[FsplayAfterEnum.TIMESTAMP.ordinal()] + ","
                                    + cityId));
                }
                catch(Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

    }

    public static class FsPlayAfterMacByCityReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Long, String> accessAreaTree = new TreeMap<Long, String>(
                    new Comparator<Long>() {
                        @Override
                        public int compare(Long o1, Long o2) {
                            // TODO Auto-generated method stub
                            return -o1.compareTo(o2);
                        }

                    });
            for (Text value : values) {
                String valueStr = value.toString();
                String[] fields = valueStr.split(",");
                try {
                    Long tmpstamp = new Long(fields[0]);
                    String cityIdStr = fields[1];
                    accessAreaTree.put(tmpstamp, cityIdStr);
                }
                catch(NumberFormatException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            StringBuilder resultValueSB = new StringBuilder();

            if (accessAreaTree.size() == 1) {
                resultValueSB.append(TimestampFormatUtil
                        .formatTimeStamp(accessAreaTree.firstEntry().getKey()
                                + ""));
                resultValueSB.append("\t");
                resultValueSB.append(accessAreaTree.firstEntry().getValue());

            }
            else if (accessAreaTree.size() > 1) {
                accessAreaTree.keySet().iterator().next();
                Long secondKeyLong = accessAreaTree.keySet().iterator().next();
                resultValueSB.append(TimestampFormatUtil
                        .formatTimeStamp(secondKeyLong + ""));
                resultValueSB.append("\t");
                resultValueSB.append(accessAreaTree.get(secondKeyLong));
            }
            resultValueSB.append("\t");
            resultValueSB
                    .append(TimestampFormatUtil.formatTimeStamp(accessAreaTree
                            .firstEntry().getKey() + ""));
            resultValueSB.append("\t");
            resultValueSB.append(accessAreaTree.firstEntry().getValue());
            context.write(key, new Text(resultValueSB.toString()));
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
