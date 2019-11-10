package com.github.atramos.jaws;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Function;

import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AwsS3Utf8StringLineSpliterator extends AwsS3LineSpliterator<String> {
	public AwsS3Utf8StringLineSpliterator(Iterator<S3ObjectSummary> objects,
			Function<S3ObjectSummary, InputStream> fileOpener, int maxBuffer) {
		super(objects, fileOpener, AwsS3Utf8StringLineSpliterator::readLine, maxBuffer);
	}
	/**
	 * Reference implementation where each LINE is represented as a UTF-8 String terminated by '\n'. 
	 * Supply your own readLine() method to customize the treatment of line-endings & character encodings,
	 * and/or obtain higher performance by replacing ByteArrayOutputStream with a more efficient alternative.
	 */
	protected static String readLine(InputStream is) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
			for (int c;;) {
				c = is.read();
				switch (c) {
				case '\n':
					return baos.toString("utf8");
				case -1:
					if (baos.size() == 0) {
						is.close();
						return null;
					} else {
						return baos.toString("utf8");
					}
				default:
					baos.write(c);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}