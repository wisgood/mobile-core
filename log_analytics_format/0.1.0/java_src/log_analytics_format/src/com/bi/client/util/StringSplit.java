/**   
 * All rights reserved
 * www.funshion.com
 *
* @Title: StringSplit.java 
* @Package com.bi.client.util 
* @Description: ����־�����д���
* @author limm
* @date 2013-9-10 ����8:46:24 
* @input:������־·��/2013-9-10
* @output:�����־·��/2013-9-10
* @executeCmd:hadoop jar ....
* @inputFormat:DateId HourId ...
* @ouputFormat:DateId MacCode ..
*/
package com.bi.client.util;

import java.util.ArrayList;
import java.util.List;

/** 
 * @ClassName: StringSplit 
 * @Description: ���ڷָ��ַ�������ֹ"\t"+"\t"+"\t"�ָ��e�`����r 
 * @date 2013-9-10 ����8:46:24  
 */
public class StringSplit {
	public static String[] splitLog(String content, char separator) {
        int length = content.length();
        int beginIndex = 0;
        List<String> pathStrings = new ArrayList<String>();
        for (int endIndex = 0; endIndex < length; endIndex++) {
            char ch = content.charAt(endIndex);
            if (ch == separator) {
                pathStrings.add(content.substring(beginIndex, endIndex));
                beginIndex = endIndex + 1;
            }
        }
        pathStrings.add(content.substring(beginIndex, length));

        return pathStrings.toArray(new String[0]);
    }
}
