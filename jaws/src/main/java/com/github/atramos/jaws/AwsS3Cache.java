package com.github.atramos.jaws;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AwsS3Cache<T> {

	public static final String CACHE_DELIMITER = "|";

	private static final int FLUSH_COUNT = 100000;

	private static ObjectMapper om = new ObjectMapper();

	protected Class<T> cls;

	protected final Logger logger = Logger.getLogger(getClass().getName());

	protected String path;

	protected AwsS3Template s3;

	private AtomicInteger dirtyCount = new AtomicInteger(0);

	private Instant lastError;

	private Map<String, String> map;

	public AwsS3Cache() {
		super();
	}

	public AwsS3Cache(String path, Class<T> cls, AwsS3Template s3) {
		this.s3 = s3;
		this.cls = cls;
		this.path = path;
	}

	public boolean containsKey(String address) {
		return map().containsKey(address.toUpperCase());
	}

	public synchronized void flush() throws IOException {
		flush(false, true);
	}

	public synchronized T get(String address, Supplier<T> cacheLoader) throws IOException {

		final String addressUC = address.toUpperCase();
		String json;

		if (!map().containsKey(addressUC)) {

			if (lastError != null) {
				if (lastError.plusSeconds(3600).isAfter(Instant.now())) {
					return null;
				}
				else {
					lastError = null;
				}
			}

			final T value;
			try {
				value = cacheLoader.get();
				if (value != null) {
					logger.info("cache-loaded [" + value + "] for address: " + address);
				}
			} catch (Exception e) {
				logger.log(Level.WARNING, address, e);
				this.lastError = Instant.now();
				return null;
			}
			json = put(addressUC, value);
		}
		else { // cache-hit
			json = map().get(addressUC);
		}

		return parse(json);
	}

	private String put(final String addressUC, final T value) throws JsonProcessingException, IOException {
		String json = om.writerFor(cls).writeValueAsString(value);
		this.put(addressUC, json);
		return json;
	}

	private T parse(String json) throws IOException, JsonParseException, JsonMappingException {
		if (json == null || json.trim().equalsIgnoreCase("null")) { return null; }
		return om.readValue(json, cls);
	}

	public void put(final String addressUC, String json) throws IOException {
		map().put(addressUC, json);
		if (dirtyCount.incrementAndGet() > FLUSH_COUNT) {
			flush();
		}
	}

	public synchronized void flush(boolean force, boolean refresh) throws IOException {
		if (force || dirtyCount.get() != 0) {

			if (refresh) {
				final Map<String, String> existing = getCache(path);
				for (Entry<String, String> e : existing.entrySet()) {
					if (e.getValue() != null) map().put(e.getKey(), e.getValue());
				}
			}

			String data = map().entrySet().stream().map(e -> {
				String address = e.getKey();
				String json = e.getValue();
				return address.replace(CACHE_DELIMITER.charAt(0), '/') + CACHE_DELIMITER + json + "\n";
			}).collect(Collectors.joining());

			s3.gzipWrite(path, data);

			dirtyCount.set(0);
		}
	}

	public synchronized Map<String, String> map() {
		if (this.map == null) {
			this.map = getCache(path);
		}
		return this.map;
	}
	
	public Stream<T> values() {
		return map().values().stream().map(json -> {
			try {
				return om.readValue(json, cls);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private synchronized Map<String, String> getCache(String path) {
		try {
			if (!s3.exists(path)) {
				logger.warning("Creating: " + path);
				return new HashMap<>();
			}
			String content = s3.gzipRead(path, false);
			final Map<String, String> collect = Arrays.stream(content.split("\n")).filter(line -> !line.isEmpty())
					.map(line -> line.split("\\" + CACHE_DELIMITER, 2))
					.collect(Collectors.toMap(a -> a[0].toUpperCase(), a -> a[1], (a, b) -> a));
			logger.info("Read " + collect.size() + " entries from " + path);
			return collect;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized <U> Map<U,T> get(Set<U> keys, Function<U,String> keyer, Function<List<U>,List<T>> func) throws JsonParseException, JsonMappingException, IOException {
		Map<String, String> tMap = map();
		List<U> remain = new ArrayList<>();
		Map<U,T> out = new HashMap<>(); 
		for(U key: keys) {
			String keyStr = keyer.apply(key);
			if(tMap.containsKey(keyStr)) {
				out.put(key, parse(tMap.get(keyStr)));
			}
			else {
				remain.add(key);
			}
		}
		logger.info("bulk get: out=" + out.size() + ", remain=" + remain.size());
		List<T> fresh = func.apply(remain);
		for(int i=0; i < fresh.size(); ++i) {
			this.put(keyer.apply(remain.get(i)), fresh.get(i));
			out.put(remain.get(i), fresh.get(i));
		}
		return out;
	}
}