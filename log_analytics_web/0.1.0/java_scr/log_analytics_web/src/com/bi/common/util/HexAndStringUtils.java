package com.bi.common.util;

import java.io.ByteArrayOutputStream;

public class HexAndStringUtils {

    /*
     * 16���������ַ���
     */
    private static String hexString = "0123456789ABCDEF"; // �˴���������Ķ�

    /*
     * ���ַ��������16��������
     */
    public static String encode(String str) {
        // ����Ĭ�ϱ����ȡ�ֽ�����
        byte[] bytes = str.getBytes();
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        // ���ֽ�������ÿ���ֽڲ���2λ16��������
        for (int i = 0; i < bytes.length; i++) {
            sb.append(hexString.charAt((bytes[i] & 0xf0) >> 4));
            sb.append(hexString.charAt((bytes[i] & 0x0f) >> 0));
        }
        return sb.toString();
    }

    /*
     * ��16�������ֽ�����ַ���
     */
    public static String decode(String bytes) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(
                bytes.length() / 2);
        // ��ÿ2λ16����������װ��һ���ֽ�
        for (int i = 0; i < bytes.length(); i += 2)
            baos.write((hexString.indexOf(bytes.charAt(i)) << 4 | hexString
                    .indexOf(bytes.charAt(i + 1))));
        return new String(baos.toByteArray());
    }

    /**
     * 
     * @Title: main
     * @Description: ������һ�仰�����������������
     * @param @param args ����˵��
     * @return void ��������˵��
     * @throws
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //System.out.println(encode("����ת��Ϊ16����")); 
        System.out.println("FSPAP".hashCode()); 
        System.out.println("FSluncher".hashCode()); 
        System.out.println(Math.abs("Fsplatform".hashCode())); 
        System.out.println("FsSvr".hashCode()); 
    }

}
