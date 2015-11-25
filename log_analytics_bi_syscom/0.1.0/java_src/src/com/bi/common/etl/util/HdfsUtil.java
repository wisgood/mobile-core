package com.bi.common.etl.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {

    /**
     * 
     * 
     * @Title: listPaths
     * @Description: 显示hdfs目录下的目录和文件
     * @param @param uri
     * @param @return 参数说明
     * @return Path[] 返回类型说明
     * @throws
     */
    public static Path[] listPaths(String uri) {

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FileStatus[] fileStatus = fs.listStatus(new Path(uri));
            return FileUtil.stat2Paths(fileStatus);
        }
        catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return new Path[0];
        }

    }

    private boolean fileExist(String uri) {

        try {
            Configuration conf = new Configuration();
            FileSystem fileSystem = null;
            fileSystem = FileSystem.get(URI.create(uri), conf);
            return fileSystem.getFileStatus(new Path(uri)) != null;
        }
        catch(IOException e) {

        }
        return false;

    }

    /**
     * 递归删除目录下的所有文件及子目录下所有文件
     * 
     * @param dir
     *            将要删除的文件目录
     * @return boolean Returns "true" if all deletions were successful. If a
     *         deletion fails, the method stops attempting to delete and returns
     *         "false".
     */
    public static void deleteDir(String uri) {

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            fs.delete(new Path(uri), true);
        }
        catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * @throws IOException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        // for (Path path :
        // listPaths("/dw/logs/toos/origin/FsPlatformBoot/3/2013/07/11"))
        // System.out.println(path.toString());


    }
}
