package com.bi.client.pgclick.format;

public enum PgclickLogEnum {
	/**
	 *hdfs·����/dw/logs/web/origin/pgclick/
	 * 
	 *1		PROTOCOL:��־Э��汾
	 *2		RPROTOCOL:����Э��汾
	 *3		TIMESTAMP:ʱ���
	 *4		IP:IP
	 *5		CLIENT_FLAG:www:1,fs:2,fsqq:3,�ƶ�web��4
	 *6		FCK:���Ψһ�û�
	 *7		MAC��Mac��ַ
	 *8		USER_ID:��½�û�ID,δ��¼Ϊ0
	 *9		fpc:���ԡ���Ӫ�̺͵����û��ĵ�ַ�����ԣ�isp��Ϣ
	 *10	VERSION:���а汾��
	 *11	SID:�ͻ�������ʱ���ɵ�ID��ÿ�� �Ự��������һ��          
	 *12	PVID:ͬһҳ��ʱ��PV�ϱ�����ͬ��ÿ��ˢ��ҳ������һ����ֵ
	 *13	CONFIG: ҳ��Ψһ��ʾ��ҳ�����
	 *14	URL:��ǰURL��ַ
	 *15	REFERURL:��ǰURL��Դurl
	 *16	CHANNEL_ID:��������ID
	 *17	BLOCK:�����ҳ��λ��
	 *18	SCREENW:��Ļ��
	 *19	SCREENH:��Ļ��
	 *20	BROWSERW:�������
	 *21	BROWSERH:�������
	 *22	BROWSERPX:�������������м�����������ĺ������꣬���Ϊ��
	 *23	BROWSERPY:�������������м��������������������
	 *24	PAGEPX:�������ҳ���м�����������ĺ������꣬���Ϊ��
	 *25	PAGEPY:�������ҳ���м��������������������
	 *26	EXT:  ��չ�ֶΣ�turnurl=?&��key=value����turnurl��ʾ�������url��  
	 *27	USERAGENT:�û��Ĳ���ϵͳ���������Ϣ                                                                                                                  
	 */
	PROTOCOL,RPROTOCOL,TIMESTAMP,IP,CLIENT_FLAG,FCK,MAC,USER_ID,FPC,VERSION,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,BLOCK,SCREENW,SCREENH,BROWSERW,BROWSERH,BROWSERPX,BROWSERPY,PAGEPX,PAGEPY,EXT,USERAGENT;
}
