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
import com.bi.ibidian.datadefine.IbidianClickEnum;
import com.bi.ibidian.datadefine.IbidianPVEnum;
import com.bi.ibidian.jargsparser.CalculateMRArgs;

public class CountExportsMR extends Configured implements Tool
{
	public static class CountExportsMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static LongWritable ONE = new LongWritable(1);
		private final static String ALLPAGES = "allpages";
		private final static String HOMEPAGE = "xidbidan";
		//private final static String HOMEPAGE2 = "ibidian.html";
		private final static String LISTPAGE = "so";
		private final static String GUDANPAGE = "compare";
		private final static String FINALPAGE = "store";

		private String enumName;
		private String[] formatFields;
		private String dateID;
		private String pageName;
		private String currentURL;
		private int notHomeIndex;
		private int curPageIsHomePageIndex;
		private int curPageIsListPageIndex;
		private int curPageIsFinalPageIndex;
		private int curPageIsGudanPageIndex;

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

			if (this.enumName.equalsIgnoreCase(CommonEnum.IBIDIANCLICKENUM.name()))
			{
				this.dateID = this.formatFields[IbidianClickEnum.T_DATEID.ordinal()];
				this.pageName = this.formatFields[IbidianClickEnum.O_PAGENAME.ordinal()];
				this.currentURL = this.formatFields[IbidianClickEnum.O_URL.ordinal()];
				this.notHomeIndex = this.currentURL.indexOf("www.ibidian");
				if (-1 == this.notHomeIndex)
				{
					StringBuilder dimsValBuf1 = new StringBuilder();
					dimsValBuf1.append(this.dateID);
					dimsValBuf1.append("\t");
					dimsValBuf1.append(ALLPAGES);
					context.write(new Text(dimsValBuf1.toString().trim()), ONE);

					if (HOMEPAGE.equals(this.pageName))
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(HOMEPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					else if (LISTPAGE.equals(this.pageName))
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(LISTPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					else if (GUDANPAGE.equals(this.pageName))
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(GUDANPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					else if (FINALPAGE.equals(this.pageName))
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(FINALPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
				}
			}
			else if (this.enumName.equalsIgnoreCase(CommonEnum.IBIDIANPVENUM.name()))
			{
				this.dateID = this.formatFields[IbidianPVEnum.T_DATEID.ordinal()];
				//this.pageName = this.formatFields[IbidianPVEnum.O_PAGENAME.ordinal()];
				this.currentURL = this.formatFields[IbidianPVEnum.O_URL.ordinal()];
				this.curPageIsHomePageIndex = this.currentURL.indexOf("ibidian.html");
				this.curPageIsListPageIndex = this.currentURL.indexOf("/so/");
				this.curPageIsGudanPageIndex = this.currentURL.indexOf("/go?");
				this.curPageIsFinalPageIndex = this.currentURL.indexOf("/store/");
				if (true)
				{
					StringBuilder dimsValBuf1 = new StringBuilder();
					dimsValBuf1.append(this.dateID);
					dimsValBuf1.append("\t");
					dimsValBuf1.append(ALLPAGES);
					context.write(new Text(dimsValBuf1.toString().trim()), ONE);

					//if (HOMEPAGE.equals(this.pageName))
					if (-1 != this.curPageIsHomePageIndex)
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(HOMEPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					//if (LISTPAGE.equals(this.pageName))
					if (-1 != this.curPageIsListPageIndex)
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(LISTPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					//if (GUDANPAGE.equals(this.pageName))
					if (-1 != this.curPageIsGudanPageIndex)
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(GUDANPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
					//if (FINALPAGE.equals(this.pageName))
					if (-1 != this.curPageIsFinalPageIndex)
					{
						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(FINALPAGE);
						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
				}
			}
		}

	}

	public static class CountExportsReduce extends Reducer<Text, LongWritable, Text, LongWritable>
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
		job.setJobName("CountExportsMR");
		job.setJarByClass(CountExportsMR.class);
		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.setMapperClass(CountExportsMap.class);
		job.setCombinerClass(CountExportsReduce.class);
		job.setReducerClass(CountExportsReduce.class);
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
			mrArgs.initParserOptions("countexportsmr.jar");
			mrArgs.parseArgs(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new CountExportsMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
