package com.bi.product.embedpage.fact;

public enum PgclickLogFormatEnum {
	/**
	 *hdfs·����/dw/logs/client/format/pgclick/
	 *
	 *1		DATE_ID
	 *2		HOUR_ID
	 *3		PROVINCE_ID
	 *4		ISP_ID
	 *5		VERSION:���а汾��
	 *6		MAC��Mac��ַ	 
	 *7		PROTOCOL:��־Э��汾
	 *8		RPROTOCOL:����Э��汾
	 *9		LONGIP
	 *10	TIMESTAMP:ʱ���
	 *11	FCK:���Ψһ�û�
	 *12	USER_ID:��½�û�ID,δ��¼Ϊ0
	 *13	FPC:���ԡ���Ӫ�̺͵����û� �ĵ�ַ�����ԣ�isp��Ϣ���ͻ���Ϊ��
	 *14	SID:�ͻ�������ʱ���ɵ�ID��ÿ�� �Ự��������һ��          
	 *15	PVID:ͬһҳ��ʱ��PV�ϱ�����ͬ��ÿ��ˢ��ҳ������һ����ֵ
	 *16	CONFIG: ҳ��Ψһ��ʾ��ҳ�����
	 *17	URL:��ǰURL��ַ
	 *18	REFERURL:��ǰURL��Դurl
	 *19	CHANNEL_ID:��������ID
	 *20	BLOCK:�����ҳ��λ��
	 *21	SCREENW:��Ļ��
	 *22	SCREENH:��Ļ��
	 *23	BROWSERW:�������
	 *24	BROWSERH:�������
	 *25	BROWSERPX:�������������м�����������ĺ������꣬���Ϊ��
	 *26	BROWSERPY:�������������м��������������������
	 *27	PAGEPX:�������ҳ���м�����������ĺ������꣬���Ϊ��
	 *28	PAGEPY:�������ҳ���м��������������������
	 *29	EXT:  ��չ�ֶΣ�turnurl=?&��key=value����turnurl��ʾ�������url��  
	 *30	USERAGENT:�û��Ĳ���ϵͳ���������Ϣ                                                                                                                  */
	DATE_ID,HOUR_ID,PROVINCE_ID,ISP_ID,VERSION_ID,MAC,PROTOCOL,RPROTOCOL,LONGIP,TIMESTAMP,FCK,USER_ID,FPC,SID,PVID,CONFIG,URL,REFERURL,CHANNEL_ID,BLOCK,SCREENW,SCREENH,BROWSERW,BROWSERH,BROWSERPX,BROWSERPY,PAGEPX,PAGEPY,EXT,USERAGENT;
}
