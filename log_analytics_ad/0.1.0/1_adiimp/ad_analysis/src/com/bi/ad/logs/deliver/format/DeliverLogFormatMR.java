package com.bi.ad.logs.deliver.format;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.bi.ad.comm.util.external.*;
import com.bi.ad.comm.util.format.*;

/**
 * MC播控日志格式:
 * <br/>
 * <a href="http://redmine.funshion.com/redmine/projects/ad-deliver-system/wiki/Hermes%E6%8E%A5%E5%8F%A3%E8%A7%84%E8%8C%83#日志存储方式"
 * >redmine wiki
 * </a>
 * <br/>
 * @author WangZhe
 * 
 */
public class DeliverLogFormatMR extends Configured implements Tool {

	public static class DeliverLogFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		private IpArea    ipArea    = null;
		private AdInfo	  adInfo    = null;
		private AdpInfo   adpInfo   = null;
		private MatInfo   matInfo   = null;
		private MediaInfo mediaInfo = null;
		private Text      newValue  = null;
		private LiveChannelInfo liveInfo = null;
		private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
		private String formatOutputDir = null;
		private String errorOutputDir  = null;
		
		public void setup(Context context) throws IOException, InterruptedException{
			newValue  = new Text();
			ipArea    = new IpArea();
			mediaInfo = new MediaInfo();
			adInfo    = new AdInfo();
			adpInfo   = new AdpInfo();
			matInfo   = new MatInfo();
			liveInfo  = new LiveChannelInfo();
			try {
				ipArea.init("ip_table");
				adInfo.init("ad_info");
				adpInfo.init("adp_info");
				matInfo.init("mat_info");
				mediaInfo.init("media_info");
				liveInfo.init("live_info");
			} catch ( IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
			
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			formatOutputDir = "format/part";
			errorOutputDir  = "error/part";
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {				
				CSVParser parser = new CSVParser();
				String[] fields = parser.parseLine(value.toString());
				String 	logType = fields[0];
				if( DeliverLog.isDeliverLog(logType)){
					DeliverLog.checkLengthEligible(logType, fields.length);
					DeliverLog deliverLog = new DeliverLog();
					deliverLog.setAdInfo(adInfo);
					deliverLog.setAdpInfo(adpInfo);
					deliverLog.setIpArea(ipArea);
					deliverLog.setLiveInfo(liveInfo);
					deliverLog.setMatInfo(matInfo);
					deliverLog.setMediaInfo(mediaInfo);
					deliverLog.setFields(fields);				
					newValue.set(deliverLog.toString());
					multipleOutputs.write(NullWritable.get(), newValue, formatOutputDir);	
				}
			} catch (Exception e) {
				newValue.set(e.getMessage() + "\t" + value.toString());
				multipleOutputs.write(NullWritable.get(), newValue, errorOutputDir);
			}
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			multipleOutputs.close();
		}

	}

	public int run(String[] args) throws Exception {	    
	    Configuration conf = getConf();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        conf = gop.getConfiguration();
        
        Job job = new Job(conf, conf.get("job_name"));
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));
        
		job.setJarByClass(DeliverLogFormatMR.class);
		job.setMapperClass(DeliverLogFormatMapper.class);		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner
				.run(new Configuration(), new DeliverLogFormatMR(), args);
		System.out.println(nRet);
	}
}
