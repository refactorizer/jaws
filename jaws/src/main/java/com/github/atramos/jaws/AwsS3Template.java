package com.github.atramos.jaws;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.JSONType;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AwsS3Template {

	private Logger logger = Logger.getLogger(getClass().getName());

	private AWSCredentialsProvider awsCredentials;

	private Region region;

	private String bucket;

	public ObjectMapper objectMapper = new ObjectMapper();
	{
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper
				.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
	}

	public String gzipRead(String path, boolean skipStaleCheck) {
		byte[] b = fetch(
				new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		try {
			return IOUtils.toString(new GZIPInputStream(bais));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String gzipRead(AwsS3FetchParams params) {
		byte[] b = fetch(params);
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		try {
			return IOUtils.toString(new GZIPInputStream(bais));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Retrieve and unzip a file from S3, returning the path to the local copy.
	 * 
	 * @throws IOException
	 * 
	 */
	public Path gzipFetch(String path,
			boolean skipStaleCheck) throws IOException {
		final byte[] b = fetch(
				new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		final Path cached
				= cacheLocation(path.replaceAll("\\.gz$", "")).toPath();
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
	public Path zipFetch(String path,
			boolean skipStaleCheck) throws IOException {
		final byte[] b = fetch(
				new AwsS3FetchParams(path).withSkipStaleCheck(skipStaleCheck));
		final Path cached
				= cacheLocation(path.replaceAll("\\.zip$", "")).toPath();

		if (cached.toFile().isFile()) {
			cached.toFile().delete();
		}
		if (!cached.toFile().exists()) {
			cached.toFile().mkdirs();
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(b);

		ZipInputStream zis = new ZipInputStream(bais);
		ZipEntry zipEntry = zis.getNextEntry();
		while (zipEntry != null) {
			String fileName = zipEntry.getName();
			logger.fine("Unzip: " + fileName);
			File newFile = new File(cached + File.separator + fileName);
			try (FileOutputStream fos = new FileOutputStream(newFile)) {
				IOUtils.copy(zis, fos);
			}
			zipEntry = zis.getNextEntry();
		}
		zis.closeEntry();
		zis.close();
		return cached;
	}

	public InputStream gzipStream(String path, boolean skipStaleCheck) {
		try {
			byte[] b = fetch(new AwsS3FetchParams(path)
					.withSkipStaleCheck(skipStaleCheck));
			ByteArrayInputStream bais = new ByteArrayInputStream(b);
			return new GZIPInputStream(bais);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	public List<JsonNode> select(AmazonS3Client s3, String path, Collection<String> columns) {
		
		final String columnExpression = columns.stream().map(c -> "s." + c).collect(Collectors.joining(", "));

		SelectObjectContentRequest request = new SelectObjectContentRequest();
		request.setBucketName(bucket);
		request.setKey(path);
		request.setExpression("select " + columnExpression + " from S3Object s");
		request.setExpressionType(ExpressionType.SQL);

		InputSerialization inputSerialization = new InputSerialization();
		inputSerialization.setJson(new JSONInput().withType(JSONType.LINES));
		inputSerialization.setCompressionType(CompressionType.GZIP);
		request.setInputSerialization(inputSerialization);

		OutputSerialization outputSerialization = new OutputSerialization();
		outputSerialization.setJson(new JSONOutput().withRecordDelimiter("\n"));
		request.setOutputSerialization(outputSerialization);
		
		try(SelectObjectContentResult result = s3.selectObjectContent(request)) {
			InputStream resultInputStream = result.getPayload().getRecordsInputStream();
			return parseJsonLines(IOUtils.toString(resultInputStream));
		} catch (SdkClientException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] fetch(AwsS3FetchParams parameterObject) {
		long started = System.currentTimeMillis();
		try {
			File cacheFile = cacheLocation(parameterObject.path);
			final boolean CACHE_FILE_EXISTS = cacheFile.exists();

			if (parameterObject.skipStaleCheck && CACHE_FILE_EXISTS) {
				if (parameterObject.cachedObjecReturnsNull)
					return null;
				else
					return Files.readAllBytes(cacheFile.toPath());
			}

			AmazonS3Client s3 = getClient();

			final GetObjectRequest getObjectRequest
					= new GetObjectRequest(bucket, parameterObject.path);

			if (CACHE_FILE_EXISTS) {
				final long fileTime = cacheFile.lastModified();
				getObjectRequest.setModifiedSinceConstraint(new Date(fileTime));
			}

			try (S3Object s3o = s3.getObject(getObjectRequest)) {

				if (s3o == null && CACHE_FILE_EXISTS) {
					logger.fine(parameterObject.path
							+ ": cached file is newer than S3");
					return Files.readAllBytes(cacheFile.toPath());
				} else {
					try (InputStream is = s3o.getObjectContent()) {
						byte[] data = IOUtils.toByteArray(is);
						logger.info(
								"fetched " + data.length + " bytes from s3://" + bucket + "/" + parameterObject.path
								+ " in " + (System.currentTimeMillis()-started)/1000.0 + "s");
						if (!parameterObject.noSave) {
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

	private FileOutputStream cacheOutputStream(File cacheFile) throws IOException {
		Files.createDirectories(cacheFile.toPath().getParent());
		return new FileOutputStream(cacheFile);
	}

	public File cacheLocation(String path) {
		File cacheDir
				= new File(System.getProperty("java.io.tmpdir"), "s3-cache");
		File cacheFile
				= new File(cacheDir, bucket + "/" + path.replace(':', '_'));
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
		byte[] buf = fetch(
				new AwsS3FetchParams(key).withSkipStaleCheck(skipStaleCheck));
		try (InputStream is = new ByteArrayInputStream(buf)) {
			InputStream is2
					= key.endsWith(".gz") ? new GZIPInputStream(is) : is;
			return objectMapper.readValue(is2, new TypeReference<T>() {
			});
		}
	}

	/**
	 * Retrieve a file from S3 (list of JSON objects) or if absent, compute and
	 * store it for future retrieval.
	 * 
	 */
	public <T> List<T> getList(String key, Class<T> cls,
			Supplier<List<T>> compute) {
		if (exists(key)) {
			return getList(key, cls, true);
		} else {
			List<T> info = compute.get();
			putList(key, cls, info);
			return info;
		}
	}

	/**
	 * Retrieve a list of JSON objects previously stored in object-per-line
	 * Athena compatible format in a given S3 key.
	 * 
	 * @param key
	 * @return
	 */
	public <T> List<T> getList(String key, Class<T> cls,
			boolean skipStaleCheck) {
		return getList(cls,
				new AwsS3FetchParams(key).withSkipStaleCheck(skipStaleCheck));
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
	public <T> List<T> getList(Class<T> cls, AwsS3FetchParams parms) {
		if (parms.nonexistentAsNull && !exists(parms.path)) {
			return new ArrayList<>();
		}
		byte[] buf = fetch(parms);
		try (InputStream is = new ByteArrayInputStream(buf)) {
			InputStream is2
					= parms.path.endsWith(".gz") ? new GZIPInputStream(is) : is;
			try (BufferedReader br
					= new BufferedReader(new InputStreamReader(is2))) {
				List<T> list = new ArrayList<T>();
				String jsonLine;
				while ((jsonLine = br.readLine()) != null) {
					list.add(objectMapper.readValue(jsonLine, cls));
				}
				return list;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Reduce memory utilization by streaming through the cache file.
	 * This method is more opinionated than the equivalent getList() method:
	 * - Assumes the file is compressed.
	 * - Caching cannot be turned off.
	 * 
	 * @param <T>
	 * @param cls
	 * @param parms
	 * @return
	 */
	public <T> Stream<T> streamThroughCache(String path, Class<T> cls, boolean skipCheck) {
		try {
			File cacheFile = cacheLocation(path);
			final boolean CACHE_FILE_EXISTS = cacheFile.exists();

			if (skipCheck && CACHE_FILE_EXISTS) {
				return unzipStream(cacheFile, cls);
			}

			AmazonS3Client s3 = getClient();

			final GetObjectRequest getObjectRequest
					= new GetObjectRequest(bucket, path);

			if (CACHE_FILE_EXISTS) {
				final long fileTime = cacheFile.lastModified();
				getObjectRequest.setModifiedSinceConstraint(new Date(fileTime));
			}

			long started = System.currentTimeMillis();
			
			try (S3Object s3o = s3.getObject(getObjectRequest)) {
				if (s3o == null && CACHE_FILE_EXISTS) {
					logger.info(path
							+ ": cached file is still valid");
					return unzipStream(cacheFile, cls);
				} else {
					Files.createDirectories(cacheFile.toPath().getParent());
					File temp = cacheLocation(path + "~temp");
					try (InputStream is = s3o.getObjectContent();
						 FileOutputStream fos = new FileOutputStream(temp)) {
						IOUtils.copy(is, fos);
						fos.close();
						cacheFile.delete();
						temp.renameTo(cacheFile); // use rename to prevent partially-written crash files
						logger.info(
								"fetched " + cacheFile.length() + " bytes from s3://" + bucket + "/" + path
								+ " in " + (System.currentTimeMillis()-started)/1000.0 + "s");
						return unzipStream(cacheFile, cls);
					}
				}
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("resource")
	protected <T> Stream<T> unzipStream(File cacheFile, Class<T> cls) throws FileNotFoundException {
		try {
			return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(cacheFile))))
					.lines()
					.map(json -> {
						try {
							return objectMapper.readValue(json, cls);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					});
		} catch (IOException e) {
			throw new RuntimeException(e);
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
				JsonNode json = objectMapper.readTree(jsonLine);
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
	 * Write objects to S3, in one-per-line Athena-compatible format or for
	 * retrieval with getList(). Writes to a local file first, which reduces
	 * memory usage and also serves as a cache. 
	 * 
	 * @param path
	 * @param list
	 * @param cls
	 * @return Returns the local cache file.
	 */
	public <T> File putList(String path, Class<T> cls, Collection<T> list) {
		ObjectWriter writerFor = objectMapper.writerFor(cls);
		File tempLocation = cacheLocation(path + "~temp");
		try(FileOutputStream cacheOut = cacheOutputStream(tempLocation);
			GZIPOutputStream gzo = new GZIPOutputStream(cacheOut)) {

			for(T item: list) {
				gzo.write(writerFor.writeValueAsBytes(item));
				gzo.write('\n');
			}
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}

		// rename-after-write to prevent creation of partially written files
		File cacheLocation = cacheLocation(path);
		cacheLocation.delete();
		tempLocation.renameTo(cacheLocation);
		
		// write to S3
		gzipMetaWrite(path, cacheLocation);
		return cacheLocation;
	}

	/**
	 * Write JSON to S3, in one-per-line Athena-compatible format or for
	 * retrieval with getList(). with Optional caching and Improved memory
	 * utilization
	 * 
	 * @param path
	 * @param list
	 */
	public void putList(String path, Collection<JsonNode> list, boolean cache) {
		try {
			byte[] ba = gzipEncode(list);
			gzipMetaWrite(path, ba);
			if (cache) {
				cacheWrite(cacheLocation(path), ba);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] gzipEncode(
			Collection<JsonNode> list) throws IOException, JsonGenerationException, JsonMappingException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzo = new GZIPOutputStream(baos);

		for (JsonNode item : list) {
			byte[] b = objectMapper.writeValueAsBytes(item);
			gzo.write(b);
			gzo.write('\n');
		}

		gzo.close();
		byte[] ba = baos.toByteArray();
		return ba;
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
	}

	/**
	 * Write an already compressed blob of json data, with appropriate
	 * meta-data.
	 * 
	 */
	public void gzipMetaWrite(String path, byte[] ba) {
		AmazonS3Client s3 = getClient();
		ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(ba.length);
		meta.setContentType("application/json");
		meta.setContentEncoding("gzip");
		s3.putObject(bucket, path, new ByteArrayInputStream(ba), meta);
		logger.info(
				"wrote " + ba.length + " bytes to s3://" + bucket + "/" + path);
	}

	public void gzipMetaWrite(String path, File file) {
		long started = System.currentTimeMillis();
		AmazonS3Client s3 = getClient();
		ObjectMetadata meta = new ObjectMetadata();
		long length = file.length();
		meta.setContentLength(length);
		meta.setContentType("application/json");
		meta.setContentEncoding("gzip");
		meta.setLastModified(new Date(file.lastModified()));
		try(FileInputStream fis = new FileInputStream(file)) {
			s3.putObject(bucket, path, fis, meta);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		logger.info(
				"wrote " + length + " bytes to s3://" + bucket + "/" + path
				+ " in " + (System.currentTimeMillis()-started)/1000.0 + "s");
	}

	public void setRegion(String region) {
		this.region = Region.getRegion(Regions.fromName(region));
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	/**
	 * Adapted from
	 * http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html
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
		final ListObjectsV2Request req = new ListObjectsV2Request()
				.withBucketName(bucket).withPrefix(prefix);
		ListObjectsV2Result result;
		do {
			result = s3client.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				out.accept(objectSummary.getKey());
			}
			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated() == true);
	}

	/**
	 * list keys with their corresponding lastModified timestamps.
	 * 
	 * @param prefix
	 * @param out
	 */
	public void listKeysWithTimestamp(String prefix, BiConsumer<String, Date> out) {
		AmazonS3Client s3client = getClient();
		final ListObjectsV2Request req = new ListObjectsV2Request()
				.withBucketName(bucket).withPrefix(prefix);
		ListObjectsV2Result result;
		do {
			result = s3client.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				out.accept(objectSummary.getKey(),
						objectSummary.getLastModified());
			}
			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated() == true);
	}

	public AmazonS3Client getClient() {
		AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
		if (region == null) {
			region = Region.getRegion(Regions
					.fromName(new DefaultAwsRegionProviderChain().getRegion()));
		}
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
	}
}
