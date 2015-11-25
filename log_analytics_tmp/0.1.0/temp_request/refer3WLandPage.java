package temp_request;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class refer3WLandPage extends Configured implements Tool
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private String analysisUrl(int levelOne, int levelTwo)
		{
			String str = null;
			if (levelOne == 1) // (www) jump to web
			{
				switch (levelTwo) {
				case 1:		str = new String("home");		break;
				case 11:	str = new String("media");		break;
				case 13:	str = new String("play");		break;
				default:	str = new String("other");
				}
			}
			return str; // if match return land area else return null
		}

		private String getLandPage(String originRecord) 
		{
			String str = null;
			String[] splitRecord = originRecord.trim().split("\t");
	
			if (splitRecord.length > 11) 
			{
				String referLevelOne = splitRecord[12];
				String urlLevelOne = splitRecord[7];
				String urlLevelTwo = splitRecord[8];
	
				if (!(referLevelOne.isEmpty()) && !(urlLevelOne.isEmpty()) && !(urlLevelTwo.isEmpty())) 
				{
					int referOne = Integer.parseInt(splitRecord[12]);
					int urlOne = Integer.parseInt(splitRecord[7]);
					int urlTwo = Integer.parseInt(splitRecord[8]);
	
					if (referOne == 1006) // refer is empty
					{
						String tmpLandPage = analysisUrl(urlOne, urlTwo);
						if (tmpLandPage != null)
							str = tmpLandPage.concat("Empty");
					} 
					else if (referOne > 1000) // refer from outside and not empty
					{
						String tmpLandPage = analysisUrl(urlOne, urlTwo);
						if (tmpLandPage != null)
							str = tmpLandPage.concat("NotEmpty");
					}
				}				
			}
			return str;// if either refer or url empty or num not enough return null else return land area
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String originRecord = value.toString().trim();
			String landPage = getLandPage(originRecord);

			if (landPage != null)
				context.write(new Text(landPage), new IntWritable(1));
		}
	}
	
	public static class Reduce extends	Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int i = 0;
			for (IntWritable value : values) 
			{
				i++;
			}
			context.write(key, new IntWritable(i));
		}
	}

	public int run(String[] argv) throws Exception
	{
		Configuration conf = getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser( conf, argv );
		conf = optionParser.getConfiguration();

		Job job = new Job(conf, "refer3WLandPage");
		job.setJarByClass(refer3WLandPage.class);

		FileInputFormat.setInputPaths( job, conf.get("inputDir") );
		FileOutputFormat.setOutputPath( job, new Path(conf.get("outputDir")) );

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;	
	}
	
	public static void main(String[] argv) throws Exception
	{
		int nRet = 0;
		nRet = ToolRunner.run( new Configuration(), new refer3WLandPage(), argv );
		System.out.println(nRet);
	}
}
