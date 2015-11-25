/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: Pv2LogEnum.java 
* @Package com.bi.client.pv2.format 
* @Description: ����־�����д���
* @author limm
* @date 2013-9-10 ����10:49:07 
* @input:������־·��/2013-9-10
* @output:�����־·��/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.pv2.format;

/** 
 * @ClassName: Pv2LogEnum
 * @hdfsaPath: /dw/logs/web/origin/pv/2 
 * @Description: pv2��־���и�ʽ˵��
 * @author limm 
 * @date 2013-9-10 ����10:49:07  
 */
public enum Pv2LogEnum {
	/**
	 * 1	protocol	��Э�������汾�ţ�ÿ��Э�鲻���κ�һ�������䶯���ð汾�Ŷ�Ҫ��������
	 * 2	rprotocol	��˽��յ�����־����Э��
	 * 3	timestamp	ʱ���
	 * 4	ip			�û�IP
	 * 5	clientFlag	www��1��fs��2��fsqq��3���ƶ�web��4
	 * 6	fck			��js�������cookie��Ψһ��ʶ�����Ա�ʶΨһ�û�
	 * 7	mac			mac��ַ����װ���пͻ��˵Ļ�������ȡ
	 * 8	userid		��¼�û�ע��id�����δ��¼Ϊ0
	 * 9	fpc			���ԡ���Ӫ�̺͵����û��ĵ�ַ�����ԣ�isp��Ϣ
	 * 10	version		���а汾��
	 * 11	sid			��ǰ�ỰID����js���ɣ��㷨��fck���ƣ��������ڶ���Ϊ30����
	 * 12	pvid		ҳ��ID��ÿ��ˢ��ҳ������һ����ֵ��UUID�㷨��
	 * 13 	config		ҳ��Ψһ��ʾ��ҳ�����
	 * 14	url			��ǰurl��ַ
	 * 15	referurl	ǰ��url
	 * 16	channelID	��������id
	 * 17	vtime		ҳ�������ʱ
	 * 18	ext			��չ�ֶ�pagetype
	 * 19	useragent	�û��Ĳ���ϵͳ���������Ϣ
	 * 20	step		��ʽ���û�ʷ��pv������������ά��
	 * 21	sestep		��ʽ������session��pv������������ά��
	 * 22	seidcount	�û�ʷ��session������������ά��
	 * 23	ta			��ʽta|ucs����ʾ��ta����|ucs�û����ࡱ��ͬ�ƶ�ͳһ��
	 */
	PROTOCOL,RPROTOCOL,TIMESTAMP,IP,CLIENT_FLAG,FCK,MAC,USER_ID,FPC,VERSION,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,VTIME,EXT,USERAGENT,STEP,SESTEP,SEIDCOUNT,TA;
}
