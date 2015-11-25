package com.bi.logconfig;

public enum LogConfInfoEnum {
	orgSplitSymbol("orgsplitsymbol"), orgfieldsLength("orgfieldslength"), filterLength(
			"filterlength"), resultSplitsSymbol("resultsplitssymbol"),resultFieldsLength("resultfieldslength"), resultValue(
			"resultvalue");
	private LogConfInfoEnum(String valueStr) {
		this.valueStr = valueStr;
	}

	private String valueStr;

	public String getValueStr() {
		return valueStr;
	}
}
