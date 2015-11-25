package com.bi.ad.test;

import java.io.IOException;
import java.util.Arrays;
import com.bi.ad.comm.util.external.*;

public class test {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String[] a = {"1", "2","30","4", "5", "3"};
		CSVParser parser = new CSVParser();
		String b = "110[0]06,[\"123,456\"],\"dev|iPad_6.1.3_iPad3.4;chan|0;res|2048*1536,dev|iPad_6.1.3_iPad3.4;chan|0;res|2048*1536\",12593,[\"ipad\",\"ipad\"],\"风行电影HD 1.5.2 rv:7.5 (iPad; iPhone OS 6.1.3; zh_CN)\"";
		System.out.println(b);
		String[] fields = parser.parseLine(b);
		for(int i=0; i<fields.length; i++){
			System.out.println(i+": "+ fields[i]);
		}
		System.out.println(fields.length);
		
//		int[] ia=new int[a.length];
//		
//		for(int i=0;i<a.length;i++){ 
//			ia[i]=Integer.parseInt(a[i]);
//		}
//		Arrays.sort(ia);
//		System.out.println(Arrays.toString(ia));
//		
//		String Str = null;
//		System.out.println(Long.valueOf(Str));	
	}

}
