package com.bi.common.dm.pojo;

import java.util.ArrayList;

public class DMURLRuleArrayList<E>{
	/**
	 * 
	 */
	private ArrayList<E> arrayList = null;

	public DMURLRuleArrayList(ArrayList<E> arrayList) {
		this.arrayList = arrayList;

	}

	public void add(E e){
		this.arrayList.add(e);
	}
	
	
	public DMURLRule getDmURLRule(String url) {
		if(url == "" || url == null){
			return null;
			
		}
		else {
			for (int i = 0; i < this.arrayList.size(); i++) {
				DMURLRule dmURLRule = (DMURLRule) this.arrayList.get(i);
				if(url.contains(dmURLRule.getUrl().trim()))
					return dmURLRule;
			}
		 }
		return null;
	}
}
