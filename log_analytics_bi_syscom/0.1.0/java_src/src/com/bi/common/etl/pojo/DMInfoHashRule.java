package com.bi.common.etl.pojo;

/**
 * IH, Serial_id, Media_id, Channel_id
 * 
 * @author fuys
 * 
 */
public class DMInfoHashRule {

	private String inforHash;
	private String serialId;
	private long mediaId;
	private long channelId;

	public DMInfoHashRule(String inforHash, String serialId, long mediaId,
			long channelId) {
		super();
		if (inforHash.length() == 32 || inforHash.length() == 40) {
			this.inforHash = inforHash;
			this.serialId = serialId;
			this.mediaId = mediaId;
			this.channelId = channelId;
		} else {
			this.inforHash = "IH";
			this.serialId = "-1";
			this.mediaId = -1l;
			this.channelId = -1l;
		}

	}

	public String getInforHash() {
		return inforHash;
	}

	public void setInforHash(String inforHash) {
		this.inforHash = inforHash;
	}

	public String getSerialId() {
		return serialId;
	}

	public void setSerialId(String serialId) {
		this.serialId = serialId;
	}

	public long getMediaId() {
		return mediaId;
	}

	public void setMediaId(long mediaId) {
		this.mediaId = mediaId;
	}

	public long getChannelId() {
		return channelId;
	}

	public void setChannelId(long channelId) {
		this.channelId = channelId;
	}

}
