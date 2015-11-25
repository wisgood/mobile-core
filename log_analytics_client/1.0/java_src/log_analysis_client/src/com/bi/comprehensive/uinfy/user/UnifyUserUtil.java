package com.bi.comprehensive.uinfy.user;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UnifyUserUtil {
    
    public static final int URL_COLUMN_ID = 32;
    public static final int SUC_COLUMN_ID = 11;
    public static enum LogType {
       PC_BOOT("/boot/", 4, '\t'),
       PC_AIRFIELD("/FsPlatformBoot/", 7, '\t'),
       PC_BROWSER("/BrowserComRun/", 7, '\t'),
       PC_SCREEN("/screensaver_start/", 3, ','),
       PC_PV("/pv/", 24, '\t'),
       MOBILE_EXIT("/f_mobile_date_exit/", 7,'\t'),
       MOBILE_BOOT("f_mobile_date_bootstrap/", 7, '\t'),
       MOBILE_PV("/mpv/",5,'\t');
       
       private final String dirName;
       private final int    columnId;
       private final char   logSeperator;
       LogType(String dir, int colId, char sep){
              dirName = dir;
              columnId = colId;
              logSeperator = sep;
       }
       public String getDirName(){
          return dirName;
       }
       public int getColumnId(){
           return columnId;
       }       
       public char getLogSeperator(){
           return logSeperator;
       }
    }
    
    public static enum  UnifyUserIndex{
           All_USER_WITH_TOOL,            // 总用户数（含工具屏保）
           ALL_USER_WITHOUT_TOOL,         // 总用户数（不含工具屏保）
           PC_CLIENT_USER_WITH_TOOL,      // PC端总用户数（含工具屏保）
           PC_CLIENT_USER_WITHOUT_TOOL,   // PC端总用户数（不含工具屏保）
           PC_CLIENT_BOOT_USER_WITH_TOOL, // PC客户端启动用户数（含工具屏保）
           MOBILE_USER,                   // 移动端总用户数
           PC_CLIENT_BOOT_USER,           // PC客户端启动用户数
           PC_TOOL_BOOT_USER,             // PC工具启动用户数
           PC_AIRFIELD_BOOT_USER,         // PC飞机场启动用户数
           PC_BROESER_BOOT_USER,          // PC浏览器组件启动用户数
           PC_SCREEN_BOOT_USER,           // PC屏保启动用户数
           PC_WEB_USER,                   // PCweb用户数
           MOBILE_APP_USER,               // 移动app用户数
           MOBILE_WEB_USER                // 移动web用户数
    }
    
    public static int getLogType(String path){
        if(path.contains(LogType.PC_BOOT.getDirName())){
            return LogType.PC_BOOT.ordinal();
        }else if(path.contains(LogType.PC_AIRFIELD.getDirName())){
            return LogType.PC_AIRFIELD.ordinal();
        }else if(path.contains(LogType.PC_BROWSER.getDirName())){
            return LogType.PC_BROWSER.ordinal();
        }else if(path.contains(LogType.PC_PV.getDirName())){
            return LogType.PC_PV.ordinal();
        }else if(path.contains(LogType.PC_SCREEN.getDirName())){
            return LogType.PC_SCREEN.ordinal();
        }else if(path.contains(LogType.MOBILE_BOOT.getDirName())){
            return LogType.MOBILE_BOOT.ordinal();
        }else if(path.contains(LogType.MOBILE_EXIT.getDirName())){
            return LogType.MOBILE_EXIT.ordinal();
        }else if(path.contains(LogType.MOBILE_PV.getDirName())){
            return LogType.MOBILE_PV.ordinal();
        }        
        return -1;       
    }
    
    public static boolean isLzoDir(String path, Configuration conf) throws IOException{
        FileStatus[] fileList = FileSystem.get(conf).listStatus(new Path(path));
        for(FileStatus filestatus : fileList){
            if(filestatus.getPath().getName().endsWith(".lzo"))
                return true;
        } 
        return false;
    }
}
