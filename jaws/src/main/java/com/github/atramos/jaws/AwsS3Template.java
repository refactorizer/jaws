package com.github.atramos.jaws;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AwsS3Template {
	
	private Logger logger = Logger.getLogger(getClass().getName());
	
	private AWSCredentialsProvider awsCredentials;
	
	private Region region;
	
	private String bucket;

	private ObjectMapper om = new ObjectMapper();
	{
		om.registerModule(new JavaTimeModule());
	}
	
	public String gzipRead(String path, boolean skipStaleCheck) throws IOException {
		byte[] b = fetch(new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		return IOUtils.toString(new GZIPInputStream(bais));
	}
	
	/**
	 * Retrieve and unzip a file from S3, returning the path to the local copy.
	 * @throws IOException 
	 * 
	 */
	public Path gzipFetch(String path, boolean skipStaleCheck) throws IOException {
		final byte[] b = fetch(new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		final Path cached = cacheLocation(path.replaceAll("\\.gz$", "")).toPath();
		final ByteArrayInputStream bais = new ByteArrayInputStream(b);
		final GZIPInputStream gzis = new GZIPInputStream(bais);
		Files.write(cached, IOUtils.toByteArray(gzis));
		return cached;
	}
	
	/**
	 * Similar to gzipFetch but works at a folder level (zip file)
	 * 
	 * @param path
	 * @param skipStaleCheck
	 * @return
	 * @throws IOException
	 */
	public Path zipFetch(String path, boolean skipStaleCheck) throws IOException {
		final byte[] b = fetch(new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		final Path cached = cacheLocation(path.replaceAll("\\.zip$", "")).toPath();
		
		if(cached.toFile().isFile()) {
			cached.toFile().delete();
		}
		if(!cached.toFile().exists()) {
			cached.toFile().mkdirs();
		}
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(b);
		
        ZipInputStream zis = new ZipInputStream(bais);
        ZipEntry zipEntry = zis.getNextEntry();
        while(zipEntry != null){
            String fileName = zipEntry.getName();
            logger.fine("Unzip: " + fileName);
            File newFile = new File(cached + File.separator + fileName);
            try(FileOutputStream fos = new FileOutputStream(newFile)) {
            	IOUtils.copy(zis, fos);
            }
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
		return cached;
	}

	public InputStream gzipStream(String path, boolean skipStaleCheck) throws IOException {
		byte[] b = fetch(new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		return new GZIPInputStream(bais);
	}

	private byte[] fetch(AwsS3FetchParams parameterObject) {
		try {
			File cacheFile = cacheLocation(parameterObject.path);
			final boolean CACHE_FILE_EXISTS = cacheFile.exists();
			
			if(parameterObject.skipStaleCheck && CACHE_FILE_EXISTS) {
				if(parameterObject.cachedObjecReturnsNull)
					return null;
				else
					return Files.readAllBytes(cacheFile.toPath());
			}

			AmazonS3Client s3 = new AmazonS3Client(awsCredentials);
			s3.setRegion(region);
			
			final GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, parameterObject.path);

			if(CACHE_FILE_EXISTS) {
				final long fileTime = cacheFile.lastModified();
				getObjectRequest.setModifiedSinceConstraint(new Date(fileTime));
			}

			try(S3Object s3o = s3.getObject(getObjectRequest)) {
				
				if(s3o == null && CACHE_FILE_EXISTS) {
					logger.fine(parameterObject.path + ": cached file is newer than S3");
					return Files.readAllBytes(cacheFile.toPath());
				}
				else {
					try (InputStream is = s3o.getObjectContent()) {
						logger.info("Fetching from S3: " + parameterObject.path);
						byte[] data = IOUtils.toByteArray(is);
						if(!parameterObject.noSave) {
							cacheWrite(cacheFile, data);
						}
						return data;
					}
				}
			}
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void cacheWrite(File cacheFile, byte[] data) throws IOException {
		Files.createDirectories(cacheFile.toPath().getParent());
		Files.write(cacheFile.toPath(), data);
	}

	public File cacheLocation(String path) {
		File cacheDir = new File(System.getProperty("java.io.tmpdir"), "s3-cache");
		File cacheFile = new File(cacheDir, bucket + "/" + path);
		return cacheFile;
	}

	/**
	 * Retrieve a single JSON object from S3.
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public <T> T get(String key, boolean skipStaleCheck) throws IOException {
		byte[] buf = fetch(new AwsS3FetchParams(key).withSkipStaleCheck(skipStaleCheck));
		try(InputStream is = new ByteArrayInputStream(buf)) {
			InputStream is2 = key.endsWith(".gz") ? new GZIPInputStream(is) : is;
			return om.readValue(is2, new TypeReference<T>(){});
		}
	}

	/**
	 * Retrieve a file from S3 (list of JSON objects) or if absent, compute and store it for future retrieval.
	 *  
	 */
	public <T> List<T> getList(String key, Class<T> cls, Supplier<List<T>> compute) {
		if(exists(key)) {
			return getList(key, cls, true);
		}
		else {
			List<T> info = compute.get();
			putList(key, cls, info);
			return info;
		}
	}
	
	/**
	 * Retrieve a list of JSON objects previously stored in object-per-line Athena compatible format in a given S3 key.
	 * 
	 * @param key
	 * @return
	 */
	public <T> List<T> getList(String key, Class<T> cls, boolean skipStaleCheck) {
		try {
			return getList(cls, new AwsS3FetchParams(key).withSkipStaleCheck(skipStaleCheck));
		} catch (IOException e) {
			throw new RuntimeException(key, e);
		}
	}
	
	/**
	 * Fully parameterized implementation of getList().
	 *  
	 * @param cls
	 * @param parms
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public <T> List<T> getList(Class<T> cls, AwsS3FetchParams parms) throws JsonParseException, JsonMappingException, IOException {
		byte[] buf = fetch(parms);
		try(InputStream is = new ByteArrayInputStream(buf)) {
			InputStream is2 = parms.path.endsWith(".gz") ? new GZIPInputStream(is) : is;
			try(BufferedReader br = new BufferedReader(new InputStreamReader(is2))) {
				List<T> list = new ArrayList<T>();
				String jsonLine;
				while ((jsonLine = br.readLine()) != null) {
					list.add(om.readValue(jsonLine, cls));
				}
				return list;
			}
		}
	}

	/**
	 * Parse a file in S3 Athena format (one json per line).
	 * 
	 * @param jsonLines
	 * @return
	 */
	public List<JsonNode> parseJsonLines(String jsonLines) {
		try {
			List<JsonNode> records = new ArrayList<>();
			BufferedReader br = new BufferedReader(new StringReader(jsonLines));
			String jsonLine;
			while ((jsonLine = br.readLine()) != null) {
				JsonNode json = om.readTree(jsonLine);
				records.add(json);
			}
			return records;
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Write objects to S3, in Athena-compatible format or for retrieval with getList().
	 * 
	 * @param path
	 * @param list
	 * @param cls
	 */
	public <T> void putList(String path, Class<T> cls, List<T> list) {
		
		String data = list.stream().map(t -> {
			try {
				return om.writerFor(cls).writeValueAsString(t);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.joining("\n"));
		
		// we can't fix S3, so any IOException is considered a fault
		try {
			gzipWrite(path, data);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Write a JSON string to S3, compressed w/ gzip.
	 * 
	 * @param path
	 * @param data
	 * @throws IOException
	 */
	public void gzipWrite(String path, String data) throws IOException {
		this.gzipWrite(path, data.getBytes());
	}

	/**
	 * Compress and write bytes, adding json-gzip metadata.
	 * 
	 * @param path
	 * @param data
	 * @throws IOException
	 */
	public void gzipWrite(String path, byte[] data) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzo = new GZIPOutputStream(baos);
		gzo.write(data);
		gzo.close();
		byte[] ba = baos.toByteArray();
		gzipMetaWrite(path, ba);
		cacheWrite(cacheLocation(path), ba);
		logger.info("wrote " + ba.length + " bytes to s3://" + bucket + "/" + path);
	}

	/**
	 * Write an already compressed blob of json data, with appropriate meta-data.
	 * 
	 */
	public void gzipMetaWrite(String path, byte[] ba) {
		AmazonS3Client s3 = getClient();
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(ba.length);
		meta.setContentType("application/json");
		meta.setContentEncoding("gzip");
		s3.putObject(bucket, path, new ByteArrayInputStream(ba), meta);
	}

	public void setRegion(String region) {
		this.region = Region.getRegion(Regions.fromName(region));
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	/**
	 * Adapted from http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html
	 *  
	 * @param prefix
	 * @return
	 */
	public List<String> listKeys(String prefix) {
		List<String> out = new ArrayList<>();
		listKeys(prefix, out::add);
		return out;
	}
	public void listKeys(String prefix, Consumer<String> out) {
		AmazonS3Client s3client = getClient();
		final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
		ListObjectsV2Result result;
		do {
			result = s3client.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				out.accept(objectSummary.getKey());
			}
			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated() == true);
	}

	public AmazonS3Client getClient() {
		AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
		s3client.setRegion(region);
		return s3client;
	}
	
	public boolean exists(String path) {
		AmazonS3Client s3client = getClient();
		return s3client.doesObjectExist(bucket, path);
	}

	public AwsS3Template(AWSCredentialsProvider awsCredentials) {
		this.awsCredentials = awsCredentials;
	}

	public AwsS3Template() {
		this(new DefaultAWSCredentialsProviderChain());
		region = Region.getRegion(Regions.fromName(new DefaultAwsRegionProviderChain().getRegion()));
	}
}
