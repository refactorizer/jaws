package com.github.atramos.jaws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class AwsCredentials implements AWSCredentialsProvider {
	private String access_key;

	private String secret_key;

	@Override
	public AWSCredentials getCredentials() {
		return new BasicAWSCredentials(access_key, secret_key);
	}

	@Override
	public void refresh() {
	}

	public void setAccess_key(String access_key) {
		this.access_key = access_key;
	}

	public void setSecret_key(String secret_key) {
		this.secret_key = secret_key;
	}
}
