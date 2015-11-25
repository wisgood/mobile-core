/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DaySummaryChannel.java 
 * @Package com.bi.client.newspecial;
 * @Description: this class is to get the following index:
 *               install_num, effective_install_num
 * @author wang haiqiang
 * @date 2013-9-10
 * @input: boot:  /dw/logs/client/format/install
 *         hs_log: /dw/logs/client/format/hs_log
 * @output: f_client_day_instl_user_date:  /dw/logs/3_client/2_user/2_day/
 * @executeCmd:hadoop jar UserDay.jar  com.bi.client.user.day.DaySummaryChannel \
 *                    /dw/logs/client/format/install/2013/08/01 \
 *                    /dw/logs/client/format/hs_log//2013/08/01 \
 *                    /dw/logs/3_client/2_user/2_day/f_client_day_instl_user_date/2013/08/01 \
 * @inputFormat:
 * args0: pgclick
 *   *1		DATE_ID
	 *2		HOUR_ID
	 *3		PROVINCE_ID
	 *4		ISP_ID
	 *5		VERSION:风行版本号
	 *6		MAC：Mac地址	 
	 *7		PROTOCOL:日志协议版本
	 *8		RPROTOCOL:请求协议版本
	 *9		LONGIP
	 *10	TIMESTAMP:时间戳
	 *11	FCK:标记唯一用户
	 *12	USER_ID:登陆用户ID,未登录为0
	 *13	FPC:策略、运营商和地域用户 的地址，策略，isp信息，客户端为空
	 *14	SID:客户端启动时生成的ID，每次 会话重新生成一个          
	 *15	PVID:同一页面时与PV上报中相同。每次刷新页面生成一个新值
	 *16	CONFIG: 页面唯一标示，页面分类
	 *17	URL:当前URL地址
	 *18	REFERURL:当前URL来源url
	 *19	CHANNEL_ID:合作渠道ID
	 *20	BLOCK:点击的页面位置
	 *21	SCREENW:屏幕宽
	 *22	SCREENH:屏幕高
	 *23	BROWSERW:浏览器宽
	 *24	BROWSERH:浏览器高
	 *25	BROWSERPX:点击距离浏览器中间线内容区域的横向坐标，左侧为负
	 *26	BROWSERPY:点击距离浏览器中间线内容区域的纵向坐标
	 *27	PAGEPX:点击距离页面中间线内容区域的横向坐标，左侧为负
	 *28	PAGEPY:点击距离页面中间线内容区域的纵向坐标
	 *29	EXT:  扩展字段，turnurl=?&（key=value）（turnurl表示点击链接url）  
	 *30	USERAGENT:用户的操作系统、浏览器信息  
 * args1: indexSpMouseMove
 *   *1		DATE_ID
	 *2		PROVINCE_ID
	 *3		ISP_ID
	 *4		MAC
	 *5		LONGIP
	 *6		TIMESTAMP:时间戳
	 *7		FCK:标记唯一用户	
	 *8		USER_ID:用户登录ID，未登录为0
	 *9		FPC:策略—运营商-地域
	 *10	CHANNEL_ID:渠道号
 * args2: pv2
     * 1	DATE_ID
	 * 2	HOUR_ID
	 * 3	PROVINCE_ID
	 * 4	ISP_ID
	 * 5	VERSION_ID
	 * 6	MAC			不为(null,""," "),默认为"-"
	 * 7	protocol	不为(null,""," "),默认为"-"
	 * 8	rprotocol	不为(null,""," "),默认为"-"
	 * 9	timestamp	时间戳,默认为0
	 * 10	LONG_IP		用户IP，默认为0
	 * 11	fck			不为(null,""," "),默认为"-"
	 * 12	userid		不为(null,""," "),默认为"-"
	 * 13	fpc			策略、运营商和地域用户的地址，策略，isp信息
	 * 14	sid			当前会话ID，由js生成，算法跟fck类似，生命周期定义为30分钟
	 * 15	pvid		页面ID，每次刷新页面生成一个新值（UUID算法）
	 * 16 	config		页面唯一标示，页面分类
	 * 17	url			当前url地址
	 * 18	referurl	前链url
	 * 19	channelID	合作渠道id
	 * 20	vtime		页面请求耗时
	 * 21	ext			扩展字段pagetype
	 * 22	useragent	用户的操作系统、浏览器信息
	 * 23	step		格式：用户史来pv计数器，各自维护
	 * 24	sestep		格式：本次session的pv计数器，各自维护
	 * 25	seidcount	用户史来session计数器，各自维护
	 * 26	ta			格式ta|ucs，表示“ta策略|ucs用户分类”（同移动统一）
 * @ouputFormat:text
 */

package com.bi.client.fact.newSpecial;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DaySummaryChannel extends Configured implements Tool {

	public static class DaySummaryChannelMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Path path;
		HashMap<String, String> channelIdMap= new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			path = fileSplit.getPath();
			ChannelIdInfo channelIdInfo = new ChannelIdInfo();
			
			channelIdInfo.init("channelId_info");
			channelIdMap=channelIdInfo.getChannelIdMap();

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringBuilder valueStringBuilder = new StringBuilder();
			StringBuilder keyStringBuilder = new StringBuilder();
			Text keyText=new Text();
			Text valueText=new Text();
			String[] inputStrings = value.toString().split("\t");
			
			String dateidString=new String();
			String urlString=new String();
			String extString=new String();
			String macString=new String();
			String channelString=new String();
			
			if (path.toString().contains("pgclick")) 
			{
				dateidString=inputStrings[0];
				urlString=inputStrings[16];
				extString=inputStrings[28];
				macString=inputStrings[5];
				channelString=inputStrings[18];
				if(urlString.matches(".*newspecial.*$"))
				{
					while(channelIdMap.containsKey(channelString))
					{
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					if(extString.matches("^.*/video/play.*$") || extString.matches("^.*turnurl=fsp.*$"))
					{
							valueStringBuilder.append("1");
							valueStringBuilder.append("\t");
					} else {
						valueStringBuilder.append("0");
						valueStringBuilder.append("\t");
					}			
					valueStringBuilder.append(macString);
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append(channelString);
				    keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
					channelString=channelIdMap.get(channelString);
					valueStringBuilder=new StringBuilder();
					keyStringBuilder=new StringBuilder();
				    }
				}
				
			} else if (path.toString().contains("indexSpMouseMove")) 
			{
				dateidString=inputStrings[0];
				channelString=inputStrings[9];
				macString=inputStrings[3];
				while(channelIdMap.containsKey(channelString))
				{
				valueStringBuilder.append("indexSpMouseMove");
				valueStringBuilder.append("\t");
				valueStringBuilder.append("1");
				valueStringBuilder.append("\t");
				valueStringBuilder.append(macString);
				keyStringBuilder.append(dateidString);
				keyStringBuilder.append("\t");
				keyStringBuilder.append(channelString);
				keyText=new Text(keyStringBuilder.toString());
				valueText=new Text(valueStringBuilder.toString());
				context.write(keyText,valueText);
				channelString=channelIdMap.get(channelString);
				valueStringBuilder=new StringBuilder();
				keyStringBuilder=new StringBuilder();
				}
				
			} else if (path.toString().contains("pv2")) 
			{
				dateidString=inputStrings[0];
				urlString=inputStrings[16];
				channelString=inputStrings[18];
				macString=inputStrings[5];
				if(urlString.matches(".*newspecial.*$"))
				{
					while(channelIdMap.containsKey(channelString))
					{
					valueStringBuilder.append("pv2");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("1");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(macString);
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append(channelString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
					channelString=channelIdMap.get(channelString);
					valueStringBuilder=new StringBuilder();
					keyStringBuilder=new StringBuilder();
					}
				}
			}
			

		}
	}

	public static class DaySummaryChannelReducer extends

	Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strTmpString=null;
			Long mousemove_numLong=(long)0;
			Long vv_numLong=(long)0;
			Integer pv_numInteger=0;
			Double vv_rateDouble=0.0;
			Double effective_vv_rateDouble=0.0;
			Double effective_pv_rateDouble=0.0;
			Long vv_user_numLong=(long)0;
			Integer pv_user_numinInteger=0;
			Integer mousemove_user_numinInteger=0;		
			Double user_play_rateDouble=0.0;
			Set<String> macSetpgclick=new HashSet<String>();
			Set<String> macSetindexSpMouseMove=new HashSet<String>();
			Set<String> macSetindexpv2=new HashSet<String>();
			Text valueText=new Text();
			StringBuilder valueStringBuilder=new StringBuilder();
			
			for (Text value : values) {
				strTmpString = value.toString().split("\t");
				if(strTmpString[0].equals("pgclick"))
				{
					vv_numLong=vv_numLong+Integer.parseInt(strTmpString[1]);
					if(strTmpString[1].equals("1") && !macSetpgclick.contains(strTmpString[2]))
					{
						macSetpgclick.add(strTmpString[2]);
					}
				}
				if(strTmpString[0].equals("indexSpMouseMove"))
				{
					mousemove_numLong=mousemove_numLong+Integer.parseInt(strTmpString[1]);
					if(!macSetindexSpMouseMove.contains(strTmpString[2]))
					{
						macSetindexSpMouseMove.add(strTmpString[2]);
					}
				}
				if(strTmpString[0].equals("pv2"))
				{
					pv_numInteger=pv_numInteger+Integer.parseInt(strTmpString[1]);
					if(!macSetindexpv2.contains(strTmpString[2]))
					{
						macSetindexpv2.add(strTmpString[2]);
					}
				}
			}
			if(pv_numInteger!=0)
			{
			vv_rateDouble=(double)Math.round(vv_numLong*10000/pv_numInteger)/10000;
			effective_pv_rateDouble=(double)Math.round(mousemove_numLong*10000/pv_numInteger)/10000;
			}
			if(mousemove_numLong!=0)
			{
			effective_vv_rateDouble=(double)Math.round(vv_numLong*10000/mousemove_numLong)/10000;
			}
			vv_user_numLong=(long)macSetpgclick.size();
			pv_user_numinInteger=macSetindexpv2.size();
			mousemove_user_numinInteger=macSetindexSpMouseMove.size();
			if(pv_user_numinInteger!=0)
			{
			user_play_rateDouble=(double)Math.round(vv_user_numLong*10000/pv_user_numinInteger)/10000;
			}
			valueStringBuilder.append(pv_numInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(vv_numLong);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(vv_rateDouble);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mousemove_numLong);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(effective_vv_rateDouble);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(effective_pv_rateDouble);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mousemove_user_numinInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(pv_user_numinInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(vv_user_numLong);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(user_play_rateDouble);
			valueText.set(valueStringBuilder.toString());
			context.write(key, valueText);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new DaySummaryChannel(), args);
		System.out.println(res);

	}


    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        Job job = new Job(conf, "DaySummaryChannel");
        job.setJarByClass(DaySummaryChannel.class);

        FileInputFormat.addInputPaths(job, conf.get("input_dirs"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));
        FileSystem.get(conf).delete(new Path(conf.get("output_dir")), true); 
        
//        FileInputFormat.addInputPath(job, new Path("/user/db2inst1/newspecial/indexSpMouseMove/*"));
//        FileInputFormat.addInputPath(job, new Path("/user/db2inst1/newspecial/pgclick/*"));
//        FileInputFormat.addInputPath(job, new Path("/user/db2inst1/newspecial/pv2/*"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/db2inst1/newspecial/output"));
//        FileSystem.get(conf).delete(new Path("/user/db2inst1/newspecial/output"), true); 
        
        job.setMapperClass(DaySummaryChannelMapper.class);
        job.setReducerClass(DaySummaryChannelReducer.class);    
		job.setOutputFormatClass(TextOutputFormat.class);
		
        job.setNumReduceTasks(10);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}