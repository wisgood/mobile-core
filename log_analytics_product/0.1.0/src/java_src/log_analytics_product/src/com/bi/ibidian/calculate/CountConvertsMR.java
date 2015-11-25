package com.bi.ibidian.calculate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.ibidian.datadefine.CommonEnum;
import com.bi.ibidian.datadefine.IbidianPVEnum;
import com.bi.ibidian.jargsparser.CalculateMRArgs;

public class CountConvertsMR extends Configured implements Tool
{
	public static class CountConvertsMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static LongWritable ONE = new LongWritable(1);
		private final static String HOMEPAGE = "ibidian.html";
		private final static String LISTPAGE = "so";
		private final static String FINALPAGE = "store";

		private String enumName;
		private String[] formatFields;
		private String dateID;
		private String currentURL;
		private String referrerURL;
		//private String pageName;
		private int curPageIsHomePageIndex;
		private int curPageIsListPageIndex;
		private int curPageIsFinalPageIndex;
		private int refPageIsHomePageIndex;
		private int refPageIsListPageIndex;

		//private int refPageIsFinalPageIndex;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			this.enumName = context.getConfiguration()
					.get(CommonEnum.ENUMNAME.name().toLowerCase());
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			this.formatFields = value.toString().split("\t");

			if (this.enumName.equalsIgnoreCase(CommonEnum.IBIDIANPVENUM.name()))
			{
				this.dateID = this.formatFields[IbidianPVEnum.T_DATEID.ordinal()];
				this.currentURL = this.formatFields[IbidianPVEnum.O_URL.ordinal()];
				this.referrerURL = this.formatFields[IbidianPVEnum.O_REFER.ordinal()];
				this.curPageIsHomePageIndex = this.currentURL.indexOf(HOMEPAGE);
				this.curPageIsListPageIndex = this.currentURL.indexOf("/so/");
				this.curPageIsFinalPageIndex = this.currentURL.indexOf("/store/");
				this.refPageIsHomePageIndex = this.referrerURL.indexOf(HOMEPAGE);
				this.refPageIsListPageIndex = this.referrerURL.indexOf("/so/");
				//this.refPageIsFinalPageIndex = this.referrerURL.indexOf("/store/");

				if (-1 != this.curPageIsHomePageIndex)
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(HOMEPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(HOMEPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
				if ((-1 != this.refPageIsHomePageIndex) && (-1 != this.curPageIsListPageIndex))
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(HOMEPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(LISTPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
				if (-1 != this.curPageIsListPageIndex)
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(LISTPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(LISTPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
				if ((-1 != this.refPageIsListPageIndex) && (-1 != this.curPageIsFinalPageIndex))
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(LISTPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(FINALPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
				/*
				if (-1 != this.refPageIsFinalPageIndex)
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(FINALPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(FINALPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
				*/
			}
			/*
			else if (this.enumName.equalsIgnoreCase(CommonEnum.IBIDIANCLICKENUM.name()))
			{
				this.dateID = this.formatFields[IbidianClickEnum.T_DATEID.ordinal()];
				this.pageName = this.formatFields[IbidianClickEnum.O_PAGENAME.ordinal()];
				this.currentURL = this.formatFields[IbidianClickEnum.O_URL.ordinal()];
				this.curPageIsHomePageIndex = this.currentURL.indexOf("www.ibidian");
				if (("store".equalsIgnoreCase(this.pageName))
						&& (-1 == this.curPageIsHomePageIndex))
				{
					StringBuilder dimsValBuf = new StringBuilder();
					dimsValBuf.append(this.dateID);
					dimsValBuf.append("\t");
					dimsValBuf.append(FINALPAGE);
					dimsValBuf.append("\t");
					dimsValBuf.append(FINALPAGE);
					context.write(new Text(dimsValBuf.toString().trim()), ONE);
				}
			}
			*/
		}
	}

	public static class CountConvertsReduce extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			long sum = 0;
			for (LongWritable val : values)
			{
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}

	}

	@Override
	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("CountConvertsMR");
		job.setJarByClass(CountConvertsMR.class);
		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.setMapperClass(CountConvertsMap.class);
		job.setCombinerClass(CountConvertsReduce.class);
		job.setReducerClass(CountConvertsReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		CalculateMRArgs mrArgs = new CalculateMRArgs();
		try
		{
			mrArgs.initParserOptions("countconvertsmr.jar");
			mrArgs.parseArgs(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new CountConvertsMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
