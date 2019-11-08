package com.github.atramos.jaws;

import java.time.Instant;

public class MetaFile {
	
	private String path;
	
	private long size;
	
	private Instant timestamp;

	public String getPath() {
		return path;
	}

	public long getSize() {
		return size;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public MetaFile(String path, long size, Instant timestamp) {
		super();
		this.path = path;
		this.size = size;
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "MetaFile [path=" + path + ", size=" + size + ", timestamp=" + timestamp + "]";
	}

}
