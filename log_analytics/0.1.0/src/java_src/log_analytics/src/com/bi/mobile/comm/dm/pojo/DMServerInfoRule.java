package com.bi.mobile.comm.dm.pojo;

public class DMServerInfoRule {

	private long serverIpLong;
	private String serverIpStr;
	private String serverEngineRoomId;
	private String serverEngineRoomName;

	public long getServerIpLong() {
		return serverIpLong;
	}

	public void setServerIpLong(long serverIpLong) {
		this.serverIpLong = serverIpLong;
	}

	public String getServerIpStr() {
		return serverIpStr;
	}

	public void setServerIpStr(String serverIpStr) {
		this.serverIpStr = serverIpStr;
	}

	public String getServerEngineRoomId() {
		return serverEngineRoomId;
	}

	public void setServerEngineRoomId(String serverEngineRoomId) {
		this.serverEngineRoomId = serverEngineRoomId;
	}

	public String getServerEngineRoomName() {
		return serverEngineRoomName;
	}

	public void setServerEngineRoomName(String serverEngineRoomName) {
		this.serverEngineRoomName = serverEngineRoomName;
	}

	public DMServerInfoRule(long serverIpLong, String serverIpStr,
			String serverEngineRoomId, String serverEngineRoomName) {
		super();
		this.serverIpLong = serverIpLong;
		this.serverIpStr = serverIpStr;
		this.serverEngineRoomId = serverEngineRoomId;
		this.serverEngineRoomName = serverEngineRoomName;
	}

}
