package com.bi.comm.calculate.frequency;

/**
 * This class is used to compute the frequency rate of the give column.
 * 
 * 
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Frequency extends Configured implements Tool
{

	/**
	 * @Description
	 * @author wangxiaowei
	 * @param args
	 */

	public int run( String[] args ) throws Exception
	{
		Configuration conf = getConf();
		conf.set( "idcolumns", args[ 2 ] );
		conf.set( "frequencycolumn", args[ 3 ] );
		Job job = new Job( conf, "frequencybycol" );
		job.setJarByClass( Frequency.class );
//		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths( job, new Path( args[ 0 ] ) );
		FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );
		job.setMapperClass( FrequencyMRUTL.FrequencyMapper.class );
		job.setReducerClass( FrequencyMRUTL.FrequencyReducer.class );
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );

		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( Text.class );

		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
		return 0;
	}

	public static void main( String[] args ) throws Exception
	{

		FrequencyArgs frequencyArgs = new FrequencyArgs();
		int nRet = 0;

		try
		{
			frequencyArgs.init( "frequencybycol.jar" );
			frequencyArgs.parse( args );
		}
		catch( Exception e )
		{
			System.out.println( e.toString() );
			System.exit( 1 );
		}

		nRet = ToolRunner.run( new Configuration(), new Frequency(), frequencyArgs.getParams() );
		System.out.println( nRet );

	}

}
