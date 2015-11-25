package com.bi.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class IOFormatUtil {
	 public static List<String> ReadLines(String filePath) {
	        ArrayList<String> lineList = new ArrayList<String>();
	        try {
	            File configFile = new File(filePath);
	            if (configFile.canRead() == true) {
	                FileInputStream fis = new FileInputStream(configFile);
	                BufferedReader br = new BufferedReader(new InputStreamReader(
	                        fis, "UTF-8"));

	                String line = null;
	                while ((line = br.readLine()) != null) {
	                    lineList.add(line);
	                }
	                fis.close();
	                br.close();
	            }
	        }
	        catch(Exception e) {
	            e.printStackTrace();
	        }

	        return lineList;
	    }
	    

	    public static List<String> ReadLinesEx(File file, String fileCoding) {
	        ArrayList<String> lineList = new ArrayList<String>();
	        try {
	            FileInputStream fis = new FileInputStream(file);
	            BufferedReader br = new BufferedReader(new InputStreamReader(fis,
	                    fileCoding));
	            String line = null;
	            while ((line = br.readLine()) != null) {
	                lineList.add(line);
	            }
	            fis.close();
	            br.close();
	        }
	        catch(IOException e) {
	            e.printStackTrace();
	            return null;
	        }

	        return lineList;
	    }

	    public static String ReadFileEx(String filePath, String fileCoding) {
	        StringBuffer buffer = new StringBuffer();

	        try {
	            File configFile = new File(filePath);
	            if (configFile.canRead() == true) {
	                FileInputStream fis = new FileInputStream(filePath);
	                BufferedReader br = new BufferedReader(new InputStreamReader(
	                        fis, fileCoding));
	                String line = null;
	                while ((line = br.readLine()) != null) {
	                    buffer.append(line);
	                    buffer.append("\n");
	                }
	                fis.close();
	                br.close();
	            }
	            else {
	                // System.out.println("file not found." + filePath);
	            }
	        }
	        catch(Exception e) {
	            e.printStackTrace();
	        }

	        return buffer.toString();
	    }

}
