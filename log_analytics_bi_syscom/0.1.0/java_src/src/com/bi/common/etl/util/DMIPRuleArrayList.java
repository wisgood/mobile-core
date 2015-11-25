package com.bi.common.etl.util;

import java.util.ArrayList;

public class DMIPRuleArrayList<E>{
	/**
	 * 
	 */
	private ArrayList<E> arrayList = null;

	public DMIPRuleArrayList(ArrayList<E> arrayList) {
		this.arrayList = arrayList;

	}

	public void add(E e){
		this.arrayList.add(e);
	}
	
	public DMIPRule getDmIPRule(long iplong) {
		if (iplong != 0) {
			for (int i = 0; i < this.arrayList.size(); i++) {
				DMIPRule dmIPRule = (DMIPRule) this.arrayList.get(i);
				if (dmIPRule.getIpLongStart() <= iplong
						&& iplong <= dmIPRule.getIpLongEnd()) {
					return dmIPRule;
				}
			}

		}
		return null;
	}
}
