package com.bi.comm.calculate.frequency;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @Description MR Progrem to compute the rate of vv
 * @author wangxiaowei
 * @date 2013-4-10 下午7:10:55
 */
public class FrequencyMRUTL
{

	private static final int	FIRST_INTERVAL		= 1;
	private static final int	SECONDE_INTERVAL	= 5;
	private static final int	THIRD_INTERVAL		= 10;
	private static final int	FOURTH_INTERVAL		= 20;
	private static final int	FIFTH_INTERVAL		= 30;
	private static final int	SIXTH_INTERVAL		= 50;
	private static final int	SEVENTH_INTERVAL	= 100;
	private static final int	EIGHTTH_INTERVAL	= 200;
	private static final int	NINETH_INTERVAL		= 500;

	/**
	 * @Description
	 * @author wangxiaowei
	 * @param args
	 */

	public static class FrequencyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private String[]	idColumns;
//
		public void setup( Context context ) throws NumberFormatException
		{

			try
			{
				idColumns = context.getConfiguration().get( "idcolumns" ).split( "," );
			}
			catch( Exception e )
			{
				e.printStackTrace();
			}
		}

		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{

			String[] line = value.toString().split( "\t" );
			StringBuilder mapOutputKey = new StringBuilder();
			for( int i = 0; i < idColumns.length; i++ )
			{
				if( i != idColumns.length - 1 )
				{
					mapOutputKey.append( line[ Integer.parseInt( idColumns[ i ] ) ] + "\t" );
				}
				else
				{
					mapOutputKey.append( line[ Integer.parseInt( idColumns[ i ] ) ] );
				}

			}
			context.write( new Text( mapOutputKey.toString() ), new Text( value.toString() ) );


		}

	}

	public static class FrequencyReducer extends Reducer<Text, Text, Text, Text>
	{

		private int	frequencyColumnIndex	;
		public void setup( Context context ) throws NumberFormatException
		{

			try
			{
				frequencyColumnIndex = Integer.parseInt( context.getConfiguration().get( "frequencycolumn" ) );
			}
			catch( Exception e )
			{
				e.printStackTrace();
			}
		}
        
	   	public void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
		{
			List<Text>[] frequencyList =  new LinkedList[10];
			for( int i = 0; i < frequencyList.length; i++ )
			{
				frequencyList[i] = new LinkedList<Text>();
			}
			int channelTotalSum = 0;
			int valueCount = 0;
			for( Text value : values )
			{
				String line[] = value.toString().split( "\t" );
				Integer columnValue = Integer.parseInt( line[ frequencyColumnIndex ] );
				frequencyList[getIntervalId( columnValue )-1].add(new Text(  value.toString()));
				channelTotalSum += columnValue;
				valueCount++;
			}
			DecimalFormat df = new DecimalFormat( "0.00000000");  
			for( int i = 0; i < frequencyList.length; i++ )
			{
				int channelIntervalSum = 0;
				for( int j = 0; j < frequencyList[i].size(); j++ )
				{
					Text value = (Text)frequencyList[i].get( j );
					int columnValue = Integer.parseInt( value.toString().split("\t" )[frequencyColumnIndex]);
					channelIntervalSum+=columnValue;
				}
				
				String media = frequencyList[i].size()+"\t"+df.format(( double )frequencyList[i].size() / valueCount) ;
				String vv =channelIntervalSum+"\t"+df.format(( double )channelIntervalSum / channelTotalSum);
				context.write( new Text( key.toString()+"\t"+(i+1)), new Text(media+"\t"+vv) );
			}
			

		}

	}

	/**
	 * @param args
	 */
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJobName( "RateMRUTL" );
		job.getConfiguration().set( "mapred.job.tracker", "local" );
		job.getConfiguration().set( "fs.default.name", "local" );
		job.setJarByClass( FrequencyMRUTL.class );
		FileInputFormat.setInputPaths( job, new Path( "input_rate" ) );
		FileOutputFormat.setOutputPath( job, new Path( "output_rate" ) );
		job.setMapperClass( FrequencyMapper.class );
		job.setReducerClass( FrequencyReducer.class );
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( Text.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( Text.class );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );

	}

	public static int getIntervalId( int columnValue )
	{
		if( columnValue == FIRST_INTERVAL )
		{
			return 1;
		}
		else if( FIRST_INTERVAL < columnValue && columnValue <= SECONDE_INTERVAL )
		{
			return 2;
		}
		else if( SECONDE_INTERVAL < columnValue && columnValue <= THIRD_INTERVAL )
		{
			return 3;
		}
		else if( THIRD_INTERVAL < columnValue && columnValue <= FOURTH_INTERVAL )
		{
			return 4;
		}
		else if( FOURTH_INTERVAL < columnValue && columnValue <= FIFTH_INTERVAL )
		{
			return 5;
		}
		else if( FIFTH_INTERVAL < columnValue && columnValue <= SIXTH_INTERVAL )
		{
			return 6;
		}
		else if( SIXTH_INTERVAL < columnValue && columnValue <= SEVENTH_INTERVAL )
		{
			return 7;
		}
		else if( SEVENTH_INTERVAL < columnValue && columnValue <= EIGHTTH_INTERVAL )
		{
			return 8;
		}
		else if( EIGHTTH_INTERVAL < columnValue && columnValue <= NINETH_INTERVAL )
		{
			return 9;
		}
		else
		{
			return 10;
		}
	}

}
