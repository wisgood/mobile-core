package com.bi.common.constrants;

public enum UtilComstrantsEnum {
	ipFormatRegex("\\d+\\.\\d+\\.\\d+\\.\\d+"), ipDefault("0.0.0.0"), mac("MAC"), macCode(
			"MAC_CODE"), macRex(
			"[0-9a-fA-F]{32}|([0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2}[:\\.][0-9a-fA-F]{2})|[0-9a-fA-F]{12}|[0-9a-fA-F]{8}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{4}[-][0-9a-fA-F]{12}|null|\\(null\\)|NULL|\\(NULL\\)"), timeStamp(
			"TIMESTAMP"), defaultHourId("00"), equalSign("="), leftParenthesis(
			"("), rightParenthesis(")"), comma(",");

	private UtilComstrantsEnum(String valueStr) {
		this.valueStr = valueStr;
	}

	private String valueStr;

	public String getValueStr() {
		return valueStr;
	}

}
