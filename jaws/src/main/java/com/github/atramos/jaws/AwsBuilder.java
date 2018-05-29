package com.github.atramos.jaws;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public class AwsBuilder {
	
	private String region;
	
	private DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
	
	public void setCredentialsProvider(DefaultAWSCredentialsProviderChain credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
	}

	public void setRegion(String region) {
		this.region = region;
	}
	
	public AmazonDynamoDB buildAmazonDynamoDB() {
		return AmazonDynamoDBClientBuilder.standard().withRegion(region).withCredentials(credentialsProvider).build();
	}

}
