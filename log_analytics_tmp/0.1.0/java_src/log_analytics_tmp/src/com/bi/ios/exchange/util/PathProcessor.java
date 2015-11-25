package com.bi.ios.exchange.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class PathProcessor
{
	public static String expendHourPaths(String dayPath)
	{
		StringBuilder hourPaths = new StringBuilder();

		for (int i = 0; i <= 23; i++)
		{
			if (23 == i)
			{
				hourPaths.append(dayPath + "/" + String.format("%02d", i));
			}
			else
			{
				hourPaths.append(dayPath + "/" + String.format("%02d", i) + ",");
			}
		}

		return hourPaths.toString().trim();
	}

	public static Path[] listHourPaths(String dayPath)
	{
		try
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(dayPath), conf);
			FileStatus[] fileStatus = fs.listStatus(new Path(dayPath));
			return FileUtil.stat2Paths(fileStatus);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return new Path[0];
		}

	}

}
