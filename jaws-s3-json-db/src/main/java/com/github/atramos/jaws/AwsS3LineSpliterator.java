package com.github.atramos.jaws;

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.atramos.jaws.AwsS3LineSpliterator.AwsS3LineInput;

/**
 * Efficient line-by-line parallel processing of S3 objects.
 */
public class AwsS3LineSpliterator<LINE> extends AbstractSpliterator<AwsS3LineInput<LINE>> {

	public final static class AwsS3LineInput<LINE> {
		final public S3ObjectSummary s3ObjectSummary;
		final public LINE lineItem;

		public AwsS3LineInput(S3ObjectSummary s3ObjectSummary, LINE lineItem) {
			this.s3ObjectSummary = s3ObjectSummary;
			this.lineItem = lineItem;
		}
	}

	private final class InputStreamHandler {
		final S3ObjectSummary file;
		final InputStream inputStream;

		InputStreamHandler(S3ObjectSummary file, InputStream is) {
			this.file = file;
			this.inputStream = is;
		}
	}

	private final Iterator<S3ObjectSummary> incomingFiles;

	private final Function<S3ObjectSummary, InputStream> fileOpener;

	private final Function<InputStream, LINE> lineReader;

	private final Deque<S3ObjectSummary> unopenedFiles;

	private final BlockingDeque<InputStreamHandler> openedFiles;

	private final Deque<AwsS3LineInput<LINE>> sharedBuffer;

	private final int maxBuffer;
	
	private static final Random rand = new Random(1);

	private AwsS3LineSpliterator(Iterator<S3ObjectSummary> incomingFiles,
			Function<S3ObjectSummary, InputStream> fileOpener, Function<InputStream, LINE> lineReader,
			Deque<S3ObjectSummary> unopenedFiles, BlockingDeque<InputStreamHandler> openedFiles,
			Deque<AwsS3LineInput<LINE>> sharedBuffer, int maxBuffer) {
		super(Long.MAX_VALUE, 0);
		this.incomingFiles = incomingFiles;
		this.fileOpener = fileOpener;
		this.lineReader = lineReader;
		this.unopenedFiles = unopenedFiles;
		this.openedFiles = openedFiles;
		this.sharedBuffer = sharedBuffer;
		this.maxBuffer = maxBuffer;
	}

	public AwsS3LineSpliterator(Iterator<S3ObjectSummary> incomingFiles,
			Function<S3ObjectSummary, InputStream> fileOpener, Function<InputStream, LINE> lineReader, int maxBuffer) {
		this(incomingFiles, fileOpener, lineReader, new ConcurrentLinkedDeque<>(), new LinkedBlockingDeque<>(), 
				new ArrayDeque<>(maxBuffer), maxBuffer);
	}

	@Override
	public boolean tryAdvance(Consumer<? super AwsS3LineInput<LINE>> action) {
		AwsS3LineInput<LINE> lineInput;
		synchronized (sharedBuffer) {
			lineInput = sharedBuffer.poll();
		}
		if (lineInput != null) {
			action.accept(lineInput);
			return true;
		}
		InputStreamHandler handle;
		try {
			handle = openedFiles.poll(rand.nextInt(2000), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		if (handle == null) {
			S3ObjectSummary unopenedFile = unopenedFiles.poll();
			if (unopenedFile == null) {
				return false;
			}
			handle = new InputStreamHandler(unopenedFile, fileOpener.apply(unopenedFile));
		}
		for (int i = 0; i < maxBuffer; ++i) {
			LINE line = lineReader.apply(handle.inputStream);
			if (line != null) {
				synchronized (sharedBuffer) {
					sharedBuffer.add(new AwsS3LineInput<LINE>(handle.file, line));
				}
			} else {
				return tryAdvance(action);
			}
		}
		openedFiles.addFirst(handle);
		return tryAdvance(action);
	}

	@Override
	public Spliterator<AwsS3LineInput<LINE>> trySplit() {
		synchronized (incomingFiles) {
			if (incomingFiles.hasNext()) {
				unopenedFiles.add(incomingFiles.next());
				return new AwsS3LineSpliterator<LINE>(incomingFiles, fileOpener, lineReader, unopenedFiles, openedFiles,
						sharedBuffer, maxBuffer);
			} else {
				return null;
			}
		}
	}
}
