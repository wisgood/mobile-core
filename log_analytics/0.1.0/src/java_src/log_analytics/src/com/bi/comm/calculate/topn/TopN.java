/**   
 * @Title: TopN.java 
 * @Package: com.bi.calculate.topn.mr 
 * @Description: 求给定列中最大的N个数(参数未)
 * @author wangxiaowei 
 * @date 2013-4-10 上午12:39:20 
 */

package com.bi.comm.calculate.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.comm.calculate.topn.TopNMRUTL.TopNMapper;
import com.bi.comm.calculate.topn.TopNMRUTL.TopNReducer;

/**
 * @Description
 * @author wangxiaowei
 * @date 2013-4-10 上午12:39:20
 */

public class TopN extends Configured implements Tool
{

	public int run( String[] args ) throws Exception
	{
		Configuration conf = getConf();
		conf.set( "idcolumns", args[ 2 ] );
		conf.set( "topcolumn", args[ 3 ] );
		conf.set( "topn", args[ 4 ] );

		Job job = new Job( conf, "topnbycol" );
		job.setJarByClass( TopN.class );

		FileInputFormat.setInputPaths( job, new Path( args[ 0 ] ) );
		FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

		job.setJobName( "TopNMRUTL" );
		job.setJarByClass( TopNMRUTL.class );
		job.setMapperClass( TopNMapper.class );
		job.setReducerClass( TopNReducer.class );
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( Text.class );
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass( NullWritable.class );
		job.setOutputValueClass( Text.class );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );

		return 0;
	}

	public static void main( String[] args ) throws Exception
	{

		TopNArgs topNArgs = new TopNArgs();
		int nRet = 0;

		try
		{
			topNArgs.init( "topn.jar" );
			topNArgs.parse( args );
		}
		catch( Exception e )
		{
			System.out.println( e.toString() );
			System.exit( 1 );
		}

		nRet = ToolRunner.run( new Configuration(), new TopN(), topNArgs.getParams() );
		System.out.println( nRet );

	}
}
