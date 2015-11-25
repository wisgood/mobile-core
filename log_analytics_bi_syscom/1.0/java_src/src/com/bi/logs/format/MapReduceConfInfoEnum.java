package com.bi.logs.format;

public enum MapReduceConfInfoEnum {

	inputPath("input"), outPutPath("output"), jobName("jobName"), reduceNum(
			"reduceNum"), isInputFormatLZOCompress("inpulzo");

	private MapReduceConfInfoEnum(String valueStr) {
		this.valueStr = valueStr;
	}

	private String valueStr;

	public String getValueStr() {
		return valueStr;
	}
}
