package com.bi.client.install.format;

public enum InstallLogFormatEnum {
	/**
	 * 1	DATE_ID:����
	 * 2	VERSION_ID:�汾
	 * 3	PROVINCE_ID:ʡ��
	 * 4	MAC:
	 * 5	ISP_ID:
	 * 6	CHANNEL_ID:����ID
	 * 7	INSTALL_TYPE:��װ���ͣ�ȡֵΪ��first|update|replace|unknown��
	 * 8	IS_AUTO:�Ƿ�������
	 * 9	PS:ppsӰ�ӣ�0Ϊ�ޣ�1Ϊ��
	 * 10	THD:Ѹ�׿�����0Ϊ�ޣ�1Ϊ��
	 * 11	PL:PPTVӰ�ӣ�0Ϊ�ޣ�1Ϊ��
	 * 12	QY:���죬0Ϊ�ޣ�1Ϊ��
	 * 13	PP:ƤƤӰ�ӣ�0Ϊ�ޣ�1Ϊ��
	 * 14	LT:���ӣ�0Ϊ�ޣ�1Ϊ��
	 * 15	QL:��Ѷ��Ƶ��0Ϊ�ޣ�1Ϊ��
	 * 16 	SV:�Ѻ���Ƶ��0Ϊ�ޣ�1Ϊ��
	 * 17	BD:�ٶ�Ӱ����0Ϊ�ޣ�1Ϊ��
	 * 18	QB:�첥��0Ϊ�ޣ�1Ϊ��
	 * 19	BQ:���磬0Ϊ�ޣ�1Ϊ��
	 * 20	AL:Alexaͳ�ƣ�0Ϊ�ޣ�1Ϊ��
	 * 21	IU:����ͳ�ƣ�0Ϊ�ޣ�1Ϊ��
	 * 22 	LONGIP
	 * 23	TIMESTAMP:
	 */
	DATE_ID,VERSION_ID,PROVINCE_ID,MAC,ISP_ID,CHANNEL_ID,INSTALL_TYPE,IS_AUTO,PS,THD,PL,QY,PP,LT,QL,SV,BD,QB,BQ,AL,IU,LONGIP,TIMESTAMP;
}
