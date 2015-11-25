package com.bi.format;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bi.client.constantEnum.ConstantEnum;
import com.bi.client.util.ChannelIdInfo;
import com.bi.client.util.IpParser;
import com.bi.client.util.MACFormat;
import com.bi.client.util.StringSplit;
import com.bi.client.util.TimeFormat;
import com.bi.common.logenum.FormatFsplatformbootEnum;
import com.bi.common.logenum.fsplatformbootEnum;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class FsplatformbootFormatMR extends Configured implements Tool{
	/**   
	 * All rights reserved
	 * www.funshion.com
	 *
	 * @Title: FsplatformbootFormatMR.java 
	 * @Package com.bi.format 
	 * @Description: format the fsplatformboot log
	 * @author limm
	 * @date 2014-01-09 
	 * @input: /dw/logs/tools/origin/FsPlatformBoot/3/ 
	 * @output: /dw/logs/format/fsplatformboot
	 * @script:     
	 * @inputFormat: PROTOCOL,RPROTOCOL,TIMESTAMP,IP,BOOT_METHOD,CLIENT_STATE,SYNC_TOOLS_STATE,MAC_CODE,GUID,
	 * 				 NAME,VERSION,OS,PULL_NAME,PULL_VERSION,QUDAO_ID;
	 * @ouputFormat: DATE_ID,HOUR_ID,PROVINCE_ID,CITY_ID,ISP_ID,QUDAO_ID,VERSION_ID,IP,MAC_CODE,PROTOCOL,
	 * 				 RPROTOCOL,BOOT_METHOD,CLIENT_STATE,SYNC_TOOL_STATE,GUID,NAME,OS,PULL_NAME,PULL_VERSION,TIMESTAMP; 
	 */
	public static class FsplatformbootFormatMapper extends
			Mapper<LongWritable, Text, NullWritable, Text>{
		private final char DEFAULT_TAB_SEP = '\t';
		private final String DEFAULT_HOUR_ID = "0";
		private final String DEFAULT_PROVINCE_ID = "-999";
		private final String DEFAULT_CITY_ID = "-999";
		private final String DEFAULT_ISP_ID = "-999";
		private final String DEFAULT_QUDAO_ID = "1";
		private final long DEFAULT_VERSION_ID = -999;
		private final String DEFAULT_IP = "0.0.0.0";
		private final String DEFAULT_MAC_CODE = "000000000000";
		private final String DEFAULT_PROTOCOL = "-999";
		private final String DEFAULT_RPROTOCOL = "-999";
		private final String DEFAULT_BOOT_METHOD = "-999";
		private final String DEFAULT_CLIENT_STATE = "-999";
		private final String DEFAULT_SYNC_TOOL_STATE = "-999";
		private final String DEFAULT_GUID = "00000000000000000000000000000000";
		private final String DEFAULT_NAME = "-";
		private final String DEFAULT_OS = "-";
		private final String DEFAULT_PULL_NAME = "-";
		private final String DEFAULT_PULL_VERSION = "-";
		private static String dateId = null;
		private IpParser ipParser = null;
		private ChannelIdInfo channelIdInfo = null;
		private HashMap<String, String> qudaoIdMap = null;
		private MultipleOutputs<NullWritable, Text> multipleOutputs = null;
		private String errorOutputDir  = null;
		private Text newValue = null;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			newValue = new Text();
			ipParser = new IpParser();
			channelIdInfo = new ChannelIdInfo();
			try{
				ipParser.init("ip_table");
				channelIdInfo.init("dm_client_channel_dimension");
				qudaoIdMap = channelIdInfo.getChannelIdMap();
				Configuration conf = context.getConfiguration();
				dateId = conf.get("stat_date");
			}catch(IOException e){
				e.printStackTrace();
				System.exit(0);
			}
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			errorOutputDir  = "_error/part";
		}
		
		public void checkLengthEligible(int length) throws Exception{
			if(length != fsplatformbootEnum.QUDAO_ID.ordinal()+1){
				throw new Exception("Fields num not match:");
			}
		}
		
		private String[] getDefaultFields(){
			String[] defaultFields = new String[FormatFsplatformbootEnum.TIMESTAMP.ordinal()+1];
			defaultFields[FormatFsplatformbootEnum.DATE_ID.ordinal()] = dateId;
			defaultFields[FormatFsplatformbootEnum.HOUR_ID.ordinal()] = DEFAULT_HOUR_ID;
			defaultFields[FormatFsplatformbootEnum.PROVINCE_ID.ordinal()] = DEFAULT_PROVINCE_ID;
			defaultFields[FormatFsplatformbootEnum.CITY_ID.ordinal()] = DEFAULT_CITY_ID;
			defaultFields[FormatFsplatformbootEnum.ISP_ID.ordinal()] = DEFAULT_ISP_ID;
			defaultFields[FormatFsplatformbootEnum.QUDAO_ID.ordinal()] = DEFAULT_QUDAO_ID;
			defaultFields[FormatFsplatformbootEnum.VERSION_ID.ordinal()] = String.valueOf(DEFAULT_VERSION_ID);
			defaultFields[FormatFsplatformbootEnum.IP.ordinal()] = DEFAULT_IP;
			defaultFields[FormatFsplatformbootEnum.MAC_CODE.ordinal()] = DEFAULT_MAC_CODE;
			defaultFields[FormatFsplatformbootEnum.PROTOCOL.ordinal()] = DEFAULT_PROTOCOL;
			defaultFields[FormatFsplatformbootEnum.RPROTOCOL.ordinal()] = DEFAULT_RPROTOCOL;
			defaultFields[FormatFsplatformbootEnum.BOOT_METHOD.ordinal()] = DEFAULT_BOOT_METHOD;
			defaultFields[FormatFsplatformbootEnum.CLIENT_STATE.ordinal()] = DEFAULT_CLIENT_STATE;
			defaultFields[FormatFsplatformbootEnum.SYNC_TOOL_STATE.ordinal()] = DEFAULT_SYNC_TOOL_STATE;
			defaultFields[FormatFsplatformbootEnum.GUID.ordinal()] = DEFAULT_GUID;
			defaultFields[FormatFsplatformbootEnum.NAME.ordinal()] = DEFAULT_NAME;
			defaultFields[FormatFsplatformbootEnum.OS.ordinal()] = DEFAULT_OS;
			defaultFields[FormatFsplatformbootEnum.PULL_NAME.ordinal()] = DEFAULT_PULL_NAME;
			defaultFields[FormatFsplatformbootEnum.PULL_VERSION.ordinal()] = DEFAULT_PULL_VERSION;
			return defaultFields;
		}
		
		private StringBuilder formatLog(String[] fields){
			StringBuilder retSB = new StringBuilder();
			String[] defaultFields = getDefaultFields();
			//dateId,hourId
			String timestamp = fields[fsplatformbootEnum.TIMESTAMP.ordinal()];
			String timeStampStr = TimeFormat.toString(timestamp);
			String[] times = timeStampStr.split("\t");
			defaultFields[FormatFsplatformbootEnum.DATE_ID.ordinal()] = times[1];
			defaultFields[FormatFsplatformbootEnum.HOUR_ID.ordinal()] = times[2];
			defaultFields[FormatFsplatformbootEnum.TIMESTAMP.ordinal()] = timestamp;
			//ip,provinceId,cityId,ispId
			String ip = IpParser.ipVerify(fields[fsplatformbootEnum.IP.ordinal()]);
			long ipLong = IpParser.ip2long(ip);
			HashMap<ConstantEnum, String> areaMap = ipParser.getAreaMap(ipLong);
			defaultFields[FormatFsplatformbootEnum.PROVINCE_ID.ordinal()] = areaMap.get(ConstantEnum.PROVINCE_ID);
			defaultFields[FormatFsplatformbootEnum.CITY_ID.ordinal()] = areaMap.get(ConstantEnum.CITY_ID);
			defaultFields[FormatFsplatformbootEnum.ISP_ID.ordinal()] = areaMap.get(ConstantEnum.ISP_ID);
			defaultFields[FormatFsplatformbootEnum.IP.ordinal()] = ip;
			//qudaoId
			String qudaoId = fields[fsplatformbootEnum.QUDAO_ID.ordinal()];
			if(qudaoIdMap.containsKey(qudaoId)){
				defaultFields[FormatFsplatformbootEnum.QUDAO_ID.ordinal()] = qudaoId;
			}
			//versionId
			String version = fields[fsplatformbootEnum.VERSION.ordinal()];
			long versionId = IpParser.ip2long(version);
			versionId = versionId == 0 ? DEFAULT_VERSION_ID : versionId;
			defaultFields[FormatFsplatformbootEnum.VERSION_ID.ordinal()] = String.valueOf(versionId);
			//macCode
			String mac = fields[fsplatformbootEnum.MAC_CODE.ordinal()];
			defaultFields[FormatFsplatformbootEnum.MAC_CODE.ordinal()] = MACFormat.macFormat(mac);
			//protocol
			String protocol = fields[fsplatformbootEnum.PROTOCOL.ordinal()];
			if(protocol.matches("^[0-9]+$")){
				defaultFields[FormatFsplatformbootEnum.PROTOCOL.ordinal()] = protocol;
			}
			//rprotocol
			String rprotocol = fields[fsplatformbootEnum.RPROTOCOL.ordinal()];
			if(rprotocol.matches("^[0-9]+$")){
				defaultFields[FormatFsplatformbootEnum.RPROTOCOL.ordinal()] = rprotocol;
			} 
			//bootMethod
			String bootMethod = fields[fsplatformbootEnum.BOOT_METHOD.ordinal()];
			if(bootMethod.matches("^[0-9]+$")){
				defaultFields[FormatFsplatformbootEnum.BOOT_METHOD.ordinal()] = bootMethod;
			}
			//clientState
			String clientState = fields[fsplatformbootEnum.CLIENT_STATE.ordinal()];
			if(clientState.matches("^[0-9]+$")){
				defaultFields[FormatFsplatformbootEnum.CLIENT_STATE.ordinal()] = clientState;
			}
			//syncToolState
			String syncToolState = fields[fsplatformbootEnum.SYNC_TOOL_STATE.ordinal()];
			if(syncToolState.matches("^[0-9]+$")){
				defaultFields[FormatFsplatformbootEnum.SYNC_TOOL_STATE.ordinal()] = syncToolState;
			}
			//guid
			String guid = fields[fsplatformbootEnum.GUID.ordinal()];
			if(guid != null && !"".equals(guid)){
				defaultFields[FormatFsplatformbootEnum.GUID.ordinal()] = guid;
			}
			//name
			String name = fields[fsplatformbootEnum.NAME.ordinal()];
			if(name != null && !"".equals(name)){
				defaultFields[FormatFsplatformbootEnum.NAME.ordinal()] = name;
			}
			//os
			String os = fields[fsplatformbootEnum.OS.ordinal()];
			if(os != null && !"".equals(os)){
				defaultFields[FormatFsplatformbootEnum.OS.ordinal()] = os;
			}
			//pullName
			String pullName = fields[fsplatformbootEnum.PULL_NAME.ordinal()];
			if(pullName != null && !"".equals(pullName)){
				defaultFields[FormatFsplatformbootEnum.PULL_NAME.ordinal()] = pullName;
			}
			//pullVersion
			String pullVersion = fields[fsplatformbootEnum.PULL_VERSION.ordinal()];
			if(pullVersion != null && !"".equals(pullVersion)){
				defaultFields[FormatFsplatformbootEnum.PULL_VERSION.ordinal()] = pullVersion;
			}
			
			for(int i=0; i<defaultFields.length; i++){
				if(i > 0){
					retSB.append(DEFAULT_TAB_SEP);
				}
				retSB.append(defaultFields[i]);
			}
			return retSB;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String originLog = value.toString();
			
			try{
				String[] fields = StringSplit.splitLog(originLog, DEFAULT_TAB_SEP);
				checkLengthEligible(fields.length);
				String formatStr = formatLog(fields).toString();	
				newValue.set(formatStr);
				context.write(NullWritable.get(), newValue);
			}catch(Exception e){
				newValue.set(e.getMessage() + "\t" + value.toString());
				multipleOutputs.write(NullWritable.get(), newValue, errorOutputDir);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException {
			multipleOutputs.close();
		}
	}
	
	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		conf = gop.getConfiguration();
		
		Job job = new Job(conf, conf.get("job_name"));
		String statDate = conf.get("stat_date");
		conf.set("stat_date", statDate);
		FileInputFormat.addInputPaths(job, conf.get("input_dir"));
		Path output = new Path(conf.get("output_dir"));
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.setJarByClass(FsplatformbootFormatMR.class);
		job.setMapperClass(FsplatformbootFormatMapper.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int nRet = ToolRunner.run(new Configuration(), new FsplatformbootFormatMR(), args);
		System.out.println(nRet);
	}
}
