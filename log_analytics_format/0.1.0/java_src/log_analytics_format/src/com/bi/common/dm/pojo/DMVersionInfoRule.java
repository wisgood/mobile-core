package com.bi.common.dm.pojo;
/**
 * 版本长整形，版本名
 * @author fuys
 *
 */
public class DMVersionInfoRule {

	private long versionLong;
	private String versionName;
	public long getVersionLong() {
		return versionLong;
	}
	public void setVersionLong(long versionLong) {
		this.versionLong = versionLong;
	}
	public String getVersionName() {
		return versionName;
	}
	public void setVersionName(String versionName) {
		this.versionName = versionName;
	}
	public DMVersionInfoRule(long versionLong, String versionName) {
		super();
		this.versionLong = versionLong;
		this.versionName = versionName;
	}

}
