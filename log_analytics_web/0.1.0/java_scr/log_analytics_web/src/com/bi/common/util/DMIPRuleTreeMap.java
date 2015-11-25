package com.bi.common.util;

import java.util.Map;
import java.util.TreeMap;

import com.bi.common.dm.pojo.DMIPRule;


public class DMIPRuleTreeMap {

	private TreeMap IPRuleTreeMap = null;
	
	public DMIPRuleTreeMap(TreeMap IPTreeMap) {
		this.IPRuleTreeMap = IPTreeMap;
	}

	public DMIPRuleTreeMap() {
		// TODO Auto-generated constructor stub
	}
	public void add(long k, com.bi.common.dm.pojo.DMIPRule ipRule){
		this.IPRuleTreeMap.put(k, ipRule);
	}
	public DMIPRule getDmIPRule( long iplong ) {
		//long keyPara = long(e); 
		if(iplong != 0){
			Map.Entry outEntry = this.IPRuleTreeMap.floorEntry( iplong );
			if( outEntry != null){
				DMIPRule outValue = (DMIPRule)outEntry.getValue();
				if(outValue.getIpLongEnd() >= iplong){
					return outValue;
				}
			}
			
		}
		return null;
	}
}