package com.bi.ad.fact.uv;

import java.util.Arrays;

public class UVFreq {
	private int maxFreq = 10;      
	private long[] arrFreq = null;
	
	UVFreq ( int maxFreq){
	    this.maxFreq = maxFreq;
	    arrFreq = new long[maxFreq];
	}
	
	public long getFreq(int i) {
		if (i >= 1 && i <= maxFreq)
			return arrFreq[i - 1];
		return 0L;
	}

	public void addFreq(int i) {
		if (i >= 1 && i <= maxFreq)
			arrFreq[i-1]++;
		else
			arrFreq[maxFreq-1]++;
	}
	
	public void addFreq(int key, int value)  {
        if (key >= 1 && key <= maxFreq)
            arrFreq[key-1] += value;
        else
            arrFreq[maxFreq-1] += value;
	}
		
	public void add(UVFreq uvFreq) {
		for (int i = 0 ; i < maxFreq; i++)
			this.arrFreq[i] += uvFreq.getFreq(i);
	}
	
	public void add(String str) {
	    String[] arrUV = str.split(",");
	    for (int i = 0 ; i < maxFreq; i++)
	        this.arrFreq[i] += Integer.parseInt(arrUV[i]);
	}
	
	public void clear(){
	    Arrays.fill(arrFreq, 0);
	}
	
	public String toString(String sep){
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(Long.toString(arrFreq[0]));
		for (int i = 1 ; i < maxFreq; i++)
			strBuilder.append(sep).append(Long.toString(arrFreq[i]));				
		return strBuilder.toString();
	}	
}
