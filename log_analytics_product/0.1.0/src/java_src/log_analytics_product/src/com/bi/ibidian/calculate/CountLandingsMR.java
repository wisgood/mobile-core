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

public class CountLandingsMR extends Configured implements Tool
{
	public static class CountLandingsMap extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static LongWritable ONE = new LongWritable(1);
		private final static String PARAM_SOURCE = "source=";

		private String enumName;
		private String[] formatFields;
		private String dateID;
		private String pageName;
		private String currentURL;
		private String referURL;
		private int paramSourcePos;
		private String sourceName;

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
				this.referURL = this.formatFields[IbidianClickEnum.O_REFER.ordinal()];
				this.paramSourcePos = this.referURL.indexOf(PARAM_SOURCE);
				if ("xidbidan".equals(this.pageName))
				{
					if (-1 != this.paramSourcePos)
					{
						this.sourceName = this.referURL.substring(this.paramSourcePos + 7);
						this.sourceName = this.cleanURLParamValue(this.sourceName);

						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(this.pageName);
						dimsValBuf.append("\t");
						dimsValBuf.append(this.sourceName);

						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
				}
			}
			else if (this.enumName.equalsIgnoreCase(CommonEnum.IBIDIANPVENUM.name()))
			{
				this.dateID = this.formatFields[IbidianPVEnum.T_DATEID.ordinal()];
				this.pageName = this.formatFields[IbidianPVEnum.O_PAGENAME.ordinal()];
				this.currentURL = this.formatFields[IbidianPVEnum.O_URL.ordinal()];
				this.paramSourcePos = this.currentURL.indexOf(PARAM_SOURCE);
				if ("xidbidan".equals(this.pageName))
				{
					if (-1 != this.paramSourcePos)
					{
						this.sourceName = this.currentURL.substring(this.paramSourcePos + 7);
						this.sourceName = this.cleanURLParamValue(this.sourceName);

						StringBuilder dimsValBuf = new StringBuilder();
						dimsValBuf.append(this.dateID);
						dimsValBuf.append("\t");
						dimsValBuf.append(this.pageName);
						dimsValBuf.append("\t");
						dimsValBuf.append(this.sourceName);

						context.write(new Text(dimsValBuf.toString().trim()), ONE);
					}
				}
			}
		}

		private String cleanURLParamValue(String paramValue)
		{
			StringBuilder cleanedValue = new StringBuilder();
			if ("".equals(paramValue.trim()) || null == paramValue)
			{
				cleanedValue.append("UNDEFINED");
			}
			char aCharacter;
			for (int i = 0; i < paramValue.length(); i++)
			{
				aCharacter = paramValue.charAt(i);
				if (Character.isLetter(aCharacter))
				{
					cleanedValue.append(aCharacter);
				}
				if ('&' == aCharacter)
				{
					break;
				}
			}
			if ("".equals(cleanedValue.toString().trim()))
			{
				cleanedValue.append("UNDEFINED");
			}
			return cleanedValue.toString().trim();
		}
	}

	public static class CountLandingsReduce extends Reducer<Text, LongWritable, Text, LongWritable>
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
		job.setJobName("CountLandingsMR");
		job.setJarByClass(CountLandingsMR.class);
		FileInputFormat.setInputPaths(job, argv[0]);
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		job.getConfiguration().set(CommonEnum.ENUMNAME.name().toLowerCase(), argv[2]);
		job.setMapperClass(CountLandingsMap.class);
		job.setCombinerClass(CountLandingsReduce.class);
		job.setReducerClass(CountLandingsReduce.class);
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
			mrArgs.initParserOptions("countlandingsmr.jar");
			mrArgs.parseArgs(args);
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			mrArgs.getAutoHelpParser().printFuncUsage();
			System.exit(1);
		}
		int nRet = 0;
		nRet = ToolRunner.run(new Configuration(), new CountLandingsMR(),
				mrArgs.getOptionValueArray());
		System.out.println(nRet);
	}

}
