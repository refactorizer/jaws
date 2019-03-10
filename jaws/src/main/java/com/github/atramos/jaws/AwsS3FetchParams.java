package com.github.atramos.jaws;

public class AwsS3FetchParams {

	public String path;

	public boolean skipStaleCheck;
	
	public boolean cachedObjecReturnsNull;

	public boolean noSave;

	public AwsS3FetchParams(String path) {
		this.path = path;
	}
	
	public AwsS3FetchParams withCachedObjectReturnsNull(boolean b) {
		cachedObjecReturnsNull = b;
		return this;
	}
	
	/**
	 * Set to true to skip the timestamp check and always return the locally cached copy.
	 * 
	 * @param b
	 * @return
	 */
	public AwsS3FetchParams withSkipStaleCheck(boolean b) {
		skipStaleCheck = b;
		return this;
	}
	
	/**
	 * When set to true, don't cache a copy of the s3 object locally.
	 *  
	 * @param b
	 * @return
	 */
	public AwsS3FetchParams withNoSave(boolean b) {
		noSave = b;
		return this;
	}
}