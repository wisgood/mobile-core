package com.bi.ad.comm.util.format;

public class MediaBase {

	MediaBase(String[] fields) {
		setMediaId(fields[0]);
		setType(fields[1]);
		setCopyright(fields[2]);
	}

	public String getMediaId() {
		return mediaId;
	}
	public void setMediaId(String mediaId) {
		this.mediaId = mediaId;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	public String getCopyright() {
		return copyright;
	}
	public void setCopyright(String copyright) {
		this.copyright = copyright;
	}

	private String mediaId   = null;
	private String type 	 = null;
	private String copyright = null;	
}