package com.bi.ad.comm.util.format;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.io.IOUtils;

public class MediaInfo {
	
	private HashMap<String, MediaBase> mediaInfo = new HashMap<String, MediaBase>();
	
	public void init(String fileName) throws IOException {
		BufferedReader in   = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String  mediaId     = null;
			MediaBase mediaBase = null;	
			String line         = null;
			String[] fields     = null;
			while ((line = in.readLine()) != null) {
			    fields  = line.trim().split("\t");
				mediaId   = fields[0];
				mediaBase = new MediaBase(fields);
				mediaInfo.put(mediaId, mediaBase);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}	
	}

	public HashMap<String, MediaBase> getMediaInfo() {
		return mediaInfo;
	}

	public void setMediaInfo(HashMap<String, MediaBase> mediaInfo) {
		this.mediaInfo = mediaInfo;
	}
		
}
