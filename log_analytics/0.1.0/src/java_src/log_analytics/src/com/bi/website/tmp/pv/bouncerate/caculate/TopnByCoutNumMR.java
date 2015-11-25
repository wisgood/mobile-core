/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: TopnByCoutNumMR.java 
 * @Package com.bi.website.tmp.pv.bouncerate.caculate 
 * @Description: 用一句话描述该文件做什么
 * @author fuys
 * @date 2013-5-23 下午5:33:33 
 */
package com.bi.website.tmp.pv.bouncerate.caculate;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @ClassName: TopnByCoutNumMR
 * @Description: 这里用一句话描述这个类的作用
 * @author fuys
 * @date 2013-5-23 下午5:33:33
 */
public class TopnByCoutNumMR {

    public static class TopnByCoutNumMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            String orgindata = value.toString();
            String[] fiedls = orgindata.split("\t");
            if (fiedls.length == 2) {
                context.write(new Text(""), value);
            }
        }

    }

    public static class TopnByCoutNumReducer extends
            Reducer<Text, Text, NullWritable, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            TreeMap<Integer, String> topNReduceResult = new TreeMap<Integer, String>(
                    new Comparator<Integer>() {

                        @Override
                        public int compare(Integer first, Integer sercond) {
                            return -first.compareTo(sercond);
                        }
                    });
            for (Text value : values) {
                String orgindata = value.toString();
                String[] fiedls = orgindata.split("\t");
                System.out.println("reduce changdu:" + fiedls.length);
                String url = fiedls[0];
                int urlCount = Integer.parseInt(fiedls[1]);
                topNReduceResult.put(urlCount, url);
                if (topNReduceResult.size() > 100) {
                    topNReduceResult.remove(topNReduceResult.lastKey());
                }
            }
            Iterator<Integer> urlCountIterator = topNReduceResult.keySet()
                    .iterator();
            while (urlCountIterator.hasNext()) {
                Integer urlcount = urlCountIterator.next();
                String urlStr = topNReduceResult.get(urlcount).trim();
                context.write(NullWritable.get(), new Text(urlStr + "\t"
                        + urlcount));
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
