/**   
 * All rights reserved
 * www.funshion.com
 *
 * @Title: DaySummary.java 
 * @Package com.bi.product.embedpage.fact;
 * @Description: this class is to get the following index of embedpage:
 *               mediapage view number(and mac), client download number(and mac),media download number(and mac),media play number
 * @author wang haiqiang
 * @date 2013-10-10
 * @input: pgclick:  /dw/logs/client/format/pgclick
 *         pv2:      /dw/logs/client/format/pv2
 * @output: /dw/logs/5_product/embed_page/DaySummary
 * @executeCmd:hadoop jar CooperativeVideo.jar  com.bi.product.embedpage.fact.DaySummary \
 *                    /dw/logs/client/format/pgclick/2013/08/01 \
 *                    /dw/logs/client/format/pv2/2013/08/01 \
 *                    /dw/logs/5_product/embed_page/DaySummary/2013/08/01 \
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
 *  args2: pv2
 *   * 1	DATE_ID
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

package com.bi.product.embedpage.fact;

import java.io.IOException;
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

public class DaySummary extends Configured implements Tool {

	public static class DaySummaryMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Path path;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			path = fileSplit.getPath();

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
			String blockString=new String();
			String macString=new String();
			String extString=new String();
			String fckString=new String();
			//1,2,3, 91mail, aphone, wdj
			//1,2,3,4,5, index,movie,varity,media,play
			if (path.toString().contains("pgclick")) 
			{
				dateidString=inputStrings[0];
				urlString=inputStrings[16];
				blockString=inputStrings[19];
				macString=inputStrings[5];
				fckString=inputStrings[10];
				extString=inputStrings[28];
				if(blockString.contains("js-slider~A") || blockString.contains("5!2~A") || blockString.contains("t_v_banner~A") || blockString.contains("t_v_src~A!2"))
				{
					if(urlString.contains("http://www.funshion.com/app/91mobile/index.html"))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=movie")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=variety")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/media.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/play.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
					
					if(urlString.contains("http://app.funshion.com/app/wdjaphone/index.html"))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=movie")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=variety")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/media.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/play.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("clientdownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
				}
			if(blockString.contains("js-slider~A") || blockString.contains("5!3~A") || blockString.contains("5!2~A") || blockString.contains("t_v_banner~A") || blockString.contains("t_v_src~A!2"))
			{
				if(urlString.contains("http://app.funshion.com/app/aphone/index.html"))
				{
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("clientdownload");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(fckString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
				} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=movie")){
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("clientdownload");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(fckString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
				} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=variety")){
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("3");
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("clientdownload");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(fckString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
				} else if(urlString.contains("http://app.funshion.com/app/aphone/media.html?mid=")){
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("4");
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("clientdownload");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(fckString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
				} else if(urlString.contains("http://app.funshion.com/app/aphone/play.html?mid=")){
					keyStringBuilder.append(dateidString);
					keyStringBuilder.append("\t");
					keyStringBuilder.append("2");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("1");
					keyStringBuilder.append("\t");
					keyStringBuilder.append("5");
					valueStringBuilder.append("pgclick");
					valueStringBuilder.append("\t");
					valueStringBuilder.append("clientdownload");
					valueStringBuilder.append("\t");
					valueStringBuilder.append(fckString);
					keyText=new Text(keyStringBuilder.toString());
					valueText=new Text(valueStringBuilder.toString());
					context.write(keyText,valueText);
				}
			}
				
				
				
					if(urlString.contains("http://www.funshion.com/app/91mobile/index.html") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl="))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=movie") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=variety") && blockString.matches("^.*t_v_list.*~3.*~A.*$") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/media.html?mid=") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/play.html?mid=") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
					if(urlString.contains("http://app.funshion.com/app/aphone/index.html") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl="))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=movie") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=variety") && blockString.matches("^.*t_v_list.*~3.*~A.*$") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/media.html?mid=") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/play.html?mid=") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
					if(urlString.contains("http://app.funshion.com/app/wdjaphone/index.html") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl="))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=movie") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=variety") && blockString.matches("^.*t_v_list.*~3.*~A.*$") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/media.html?mid=") && (blockString.matches("^.*t_v_list.*~3.*~A.*$") || blockString.contains("dbtn_big")) && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/play.html?mid=") && blockString.contains("dbtn_big") && extString.equals("turnurl=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pgclick");
						valueStringBuilder.append("\t");
						valueStringBuilder.append("mediadownload");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(fckString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
					if(extString.contains("turnurl=http://www.funshion.com/app/91mobile/play.html?mid="))
					{
						if(urlString.contains("http://www.funshion.com/app/91mobile/index.html"))
						{
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=movie")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=variety")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://www.funshion.com/app/91mobile/media.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("4");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://www.funshion.com/app/91mobile/play.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("5");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						}
						
					} else if(extString.contains("turnurl=http://app.funshion.com/app/aphone/play.html?mid="))
					{
						keyStringBuilder=new StringBuilder();
						valueStringBuilder=new StringBuilder();
						if(urlString.contains("http://app.funshion.com/app/aphone/index.html"))
						{
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=movie")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=variety")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/aphone/media.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("4");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/aphone/play.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("5");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						}
					} else if(extString.contains("turnurl=http://app.funshion.com/app/wdjaphone/play.html?mid="))
					{
						keyStringBuilder=new StringBuilder();
						valueStringBuilder=new StringBuilder();
						if(urlString.contains("http://app.funshion.com/app/wdjaphone/index.html"))
						{
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=movie")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("2");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=variety")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/media.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("4");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/play.html?mid=")){
							keyStringBuilder.append(dateidString);
							keyStringBuilder.append("\t");
							keyStringBuilder.append("3");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("1");
							keyStringBuilder.append("\t");
							keyStringBuilder.append("5");
							valueStringBuilder.append("pgclick");
							valueStringBuilder.append("\t");
							valueStringBuilder.append("mediaplay");
							valueStringBuilder.append("\t");
							valueStringBuilder.append(fckString);
							keyText=new Text(keyStringBuilder.toString());
							valueText=new Text(valueStringBuilder.toString());
							context.write(keyText,valueText);
						}
					}

				
				
			} else if (path.toString().contains("pv2")) 
			{
				dateidString=inputStrings[0];
				urlString=inputStrings[16];
				macString=inputStrings[5];
				
					if(urlString.contains("http://www.funshion.com/app/91mobile/index.html"))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=movie")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/list.html?type=variety")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/media.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://www.funshion.com/app/91mobile/play.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}

					if(urlString.contains("http://app.funshion.com/app/aphone/index.html"))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=movie")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/list.html?type=variety")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/media.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/aphone/play.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
					if(urlString.contains("http://app.funshion.com/app/wdjaphone/index.html"))
					{
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=movie")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("2");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/list.html?type=variety")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/media.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("4");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					} else if(urlString.contains("http://app.funshion.com/app/wdjaphone/play.html?mid=")){
						keyStringBuilder.append(dateidString);
						keyStringBuilder.append("\t");
						keyStringBuilder.append("3");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("1");
						keyStringBuilder.append("\t");
						keyStringBuilder.append("5");
						valueStringBuilder.append("pv2");
						valueStringBuilder.append("\t");
						valueStringBuilder.append(macString);
						keyText=new Text(keyStringBuilder.toString());
						valueText=new Text(valueStringBuilder.toString());
						context.write(keyText,valueText);
					}
				} 
			}
	}

	public static class DaySummaryReducer extends

	Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strTmpString = null;
			Integer mediapageInteger = 0;
			Integer mediapagemacInteger = 0;
			Integer clientdownloadInteger = 0;
			Integer clientdownloadfckInteger = 0;
			Integer mediadownloadInteger = 0;
			Integer mediadownloadfckInteger = 0;
			Integer mediaplayInteger = 0;
			Set<String> mediapagemac = new HashSet<String>();
			Set<String> clientdownloadfck = new HashSet<String>();
			Set<String> mediadownloadfck = new HashSet<String>();
			Text valueText = new Text();
			StringBuilder valueStringBuilder = new StringBuilder();

			for (Text value : values) {
				strTmpString = value.toString().split("\t");
				if (strTmpString[0].equals("pv2")) {
					mediapageInteger = mediapageInteger+ 1;
						if (!mediapagemac.contains(strTmpString[1])) 
						{
							mediapagemac.add(strTmpString[1]);
					  }
				}
				if (strTmpString[0].equals("pgclick")) {
					if(strTmpString[1].equals("clientdownload"))
					{
						clientdownloadInteger = clientdownloadInteger+ 1;
						if (!clientdownloadfck.contains(strTmpString[2])) 
						{
							clientdownloadfck.add(strTmpString[2]);
					     }
					} else if (strTmpString[1].equals("mediadownload"))
					{
						mediadownloadInteger=mediadownloadInteger+1;
						if (!mediadownloadfck.contains(strTmpString[2])) 
						{
							mediadownloadfck.add(strTmpString[2]);
					     }
					} else if (strTmpString[1].equals("mediaplay"))
					{
						mediaplayInteger=mediaplayInteger+1;
					}
				}
			}
			mediapagemacInteger = mediapagemac.size();
			clientdownloadfckInteger= clientdownloadfck.size();
			mediadownloadfckInteger=mediadownloadfck.size();
			valueStringBuilder.append(mediapageInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mediapagemacInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(clientdownloadInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(clientdownloadfckInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mediadownloadInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mediadownloadfckInteger);
			valueStringBuilder.append("\t");
			valueStringBuilder.append(mediaplayInteger);
			valueText.set(valueStringBuilder.toString());
			context.write(key, valueText);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new DaySummary(), args);
		System.out.println(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		Job job = new Job(conf, "DaySummary");
		job.setJarByClass(DaySummary.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileSystem.get(conf).delete(new Path(args[2]), true);

		job.setMapperClass(DaySummaryMapper.class);
		job.setReducerClass(DaySummaryReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}