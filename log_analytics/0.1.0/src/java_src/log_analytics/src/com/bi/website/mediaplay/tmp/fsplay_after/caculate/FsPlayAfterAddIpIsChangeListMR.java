/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: FsPlayAfterAddIpIsChangeListMR.java 
 * @Package com.bi.website.mediaplay.tmp.fsplay_after.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-6-6 上午10:39:57 
 */
package com.bi.website.mediaplay.tmp.fsplay_after.caculate;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.bi.client.fsplayafter.format.dataenum.FsplayAfterEnum;
import com.bi.comm.util.IPFormatUtil;
import com.bi.mobile.comm.constant.ConstantEnum;
import com.bi.mobile.comm.dm.pojo.dao.AbstractDMDAO;
import com.bi.mobile.comm.dm.pojo.dao.DMIPRuleDAOImpl;

/**
 * @ClassName: FsPlayAfterAddIpIsChangeListMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-6-6 上午10:39:57
 */
public class FsPlayAfterAddIpIsChangeListMR {

    private static final String RATE_PATH = "fsplayafternetflowrate";

    private static final String ORGIN_PATH = "oxeye";

    public static class FsPlayAfterAddIpIsChangeListMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String filePathStr = null;

        private AbstractDMDAO<Long, Map<ConstantEnum, String>> dmIPRuleDAO = null;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            this.dmIPRuleDAO = new DMIPRuleDAOImpl<Long, Map<ConstantEnum, String>>();
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            this.filePathStr = fileInputSplit.getPath().toUri().getPath();
            dmIPRuleDAO.parseDMObj(new File(ConstantEnum.IP_TABLE.name()
                    .toLowerCase()));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub

            String valueStr = value.toString();

            if (this.filePathStr
                    .contains(FsPlayAfterAddIpIsChangeListMR.RATE_PATH)) {

                String[] fields = valueStr.split("\t");
                context.write(new Text(fields[0]), new Text(
                        FsPlayAfterAddIpIsChangeListMR.RATE_PATH + valueStr));

            }
            else if (this.filePathStr
                    .contains(FsPlayAfterAddIpIsChangeListMR.ORGIN_PATH)) {
                String[] fields = valueStr.split(",");
                if (fields.length > FsplayAfterEnum.MAC.ordinal()) {
                    try {
                        long ipLong = 0;
                        ipLong = IPFormatUtil.ip2long(fields[FsplayAfterEnum.IP
                                .ordinal()]);
                        java.util.Map<ConstantEnum, String> ipRuleMap;
                        ipRuleMap = dmIPRuleDAO.getDMOjb(ipLong);
                        String cityId = ipRuleMap.get(ConstantEnum.CITY_ID);
                        context.write(
                                new Text(fields[FsplayAfterEnum.MAC.ordinal()]),
                                new Text(
                                        FsPlayAfterAddIpIsChangeListMR.ORGIN_PATH
                                                + cityId));
                    }
                    catch(Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

            }

        }
    }

    public static class FsPlayAfterAddIpIsChangeListReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            StringBuilder resultValueSB = new StringBuilder();
            Set<String> iptables = new HashSet<String>();
            boolean isreate = false;
            for (Text value : values) {
                String valueStr = value.toString();

                if (valueStr.contains(FsPlayAfterAddIpIsChangeListMR.RATE_PATH)) {
                    resultValueSB.append(valueStr
                            .substring(FsPlayAfterAddIpIsChangeListMR.RATE_PATH
                                    .length()));
                    isreate = true;
                }
                else {
                    iptables.add(valueStr
                            .substring(FsPlayAfterAddIpIsChangeListMR.ORGIN_PATH
                                    .length()));
                }

            }
            if (isreate) {
                int iptalesLength = iptables.size();
                if (iptalesLength > 1) {

                    resultValueSB.append("\t");
                    resultValueSB.append("isChange");
                }
                else {
                    resultValueSB.append("\t");
                    resultValueSB.append("isNotChange");
                }
                resultValueSB.append("\t");
                resultValueSB.append("[");
                Iterator<String> iterator = iptables.iterator();
                for (int i = 0; iterator.hasNext(); i++) {
                    resultValueSB.append(iterator.next());
                    if (i < iptalesLength - 1) {
                        resultValueSB.append(",");
                    }
                }
                resultValueSB.append("]");
                context.write(new Text(resultValueSB.toString()), new Text(""));
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
