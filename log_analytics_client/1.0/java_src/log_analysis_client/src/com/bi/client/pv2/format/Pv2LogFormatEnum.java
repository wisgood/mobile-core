/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2LogFormatEnum.java 
* @Package com.bi.client.pv2.format 
* @Description: ����־�����д���
* @author limm
* @date 2013-9-10 ����3:30:16 
* @input:������־·��/2013-9-10
* @output:�����־·��/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

/** 
 * @ClassName: Pv2LogFormatEnum 
 * @Description: pv2��־Format֮�����˵��
 * @hdfsPath��/dw/logs/client/format/pv2 
 * @author limm 
 * @date 2013-9-10 ����3:30:16  
 */
public enum Pv2LogFormatEnum {
	/**
	 * 1	DATE_ID
	 * 2	HOUR_ID
	 * 3	PROVINCE_ID
	 * 4	ISP_ID
	 * 5	VERSION_ID
	 * 6	MAC			��Ϊ(null,""," "),Ĭ��Ϊ"-"
	 * 7	protocol	��Ϊ(null,""," "),Ĭ��Ϊ"-"
	 * 8	rprotocol	��Ϊ(null,""," "),Ĭ��Ϊ"-"
	 * 9	timestamp	ʱ���,Ĭ��Ϊ0
	 * 10	LONG_IP		�û�IP��Ĭ��Ϊ0
	 * 11	fck			��Ϊ(null,""," "),Ĭ��Ϊ"-"
	 * 12	userid		��Ϊ(null,""," "),Ĭ��Ϊ"-"
	 * 13	fpc			���ԡ���Ӫ�̺͵����û��ĵ�ַ�����ԣ�isp��Ϣ
	 * 14	sid			��ǰ�ỰID����js���ɣ��㷨��fck���ƣ��������ڶ���Ϊ30����
	 * 15	pvid		ҳ��ID��ÿ��ˢ��ҳ������һ����ֵ��UUID�㷨��
	 * 16 	config		ҳ��Ψһ��ʾ��ҳ�����
	 * 17	url			��ǰurl��ַ
	 * 18	referurl	ǰ��url
	 * 19	channelID	��������id
	 * 20	vtime		ҳ�������ʱ
	 * 21	ext			��չ�ֶ�pagetype
	 * 22	useragent	�û��Ĳ���ϵͳ���������Ϣ
	 * 23	step		��ʽ���û�ʷ��pv������������ά��
	 * 24	sestep		��ʽ������session��pv������������ά��
	 * 25	seidcount	�û�ʷ��session������������ά��
	 * 26	ta			��ʽta|ucs����ʾ��ta����|ucs�û����ࡱ��ͬ�ƶ�ͳһ��
	 */
	DATE_ID,HOUR_ID,PROVINCE_ID,ISP_ID,VERSION_ID,MAC,PROTOCOL,RPROTOCOL,TIMESTAMP,LONGIP,FCK,USER_ID,FPC,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,VTIME,EXT,USERAGENT,STEP,SESTEP,SEIDCOUNT,TA;
}
