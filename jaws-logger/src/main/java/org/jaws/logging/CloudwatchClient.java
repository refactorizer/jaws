package org.jaws.logging;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;

public class CloudwatchClient {

	protected static CloudwatchClient instance;

	public static synchronized CloudwatchClient getInstance() {
		if (instance == null) {
			instance = new CloudwatchClient();
		}
		return instance;
	}

	/**
	 * The queue used to buffer log entries
	 */
	private LinkedBlockingQueue<LogRecord> loggingEventsQueue
                = new LinkedBlockingQueue<>();

	/**
	 * the AWS Cloudwatch Logs API client
	 */
	protected AWSLogs awsLogsClient;

	private Formatter formatter = new SimpleFormatter();

	private AtomicReference<String> lastSequenceToken = new AtomicReference<>();

	/**
	 * The AWS Cloudwatch Log group name
	 */
	private String logGroupName;

	/**
	 * The AWS Cloudwatch Log stream name
	 */
	private String logStreamName;

	/**
	 * The maximum number of log entries to send in one go to the AWS Cloudwatch
	 * Log service
	 */
	private final static int messagesBatchSize = 128;

	private ScheduledThreadPoolExecutor exe;

	public CloudwatchClient() {
		super();
		try {
			init(System.getProperty("LOG_GROUP", "/default"), "default");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Alternate constructor required for unit-test.
	 * 
	 * @param formatter
	 * @param logGroup
	 * @param logStream
	 */
	protected CloudwatchClient(Formatter formatter, String logGroup, String logStream) {
		try {
			this.formatter = formatter;
			init(logGroup, logStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getLogGroupName() {
		return logGroupName;
	}

	private synchronized void sendMessages() {
		LogRecord polledLoggingEvent;
		List<LogRecord> loggingEvents = new ArrayList<>();
		try {
			while ((polledLoggingEvent = loggingEventsQueue.poll()) != null
					&& loggingEvents.size() <= messagesBatchSize) {
				loggingEvents.add(polledLoggingEvent);
			}

			List<InputLogEvent> inputLogEvents = loggingEvents.stream()
					.sorted(Comparator.comparing(LogRecord::getMillis))
					.map(loggingEvent -> {
						String msg;
						if (loggingEvent instanceof JsonLogRecord) {
							msg = ((JsonLogRecord) loggingEvent).getMessage();
						} else {
							msg = formatter.format(loggingEvent);
						}
						return new InputLogEvent()
								.withTimestamp(loggingEvent.getMillis())
								.withMessage(msg);
					}).collect(toList());

			if (!loggingEvents.isEmpty()) {
				PutLogEventsRequest putLogEventsRequest
						= new PutLogEventsRequest(logGroupName, logStreamName,
								inputLogEvents);
				try {
					putLogEventsRequest
							.setSequenceToken(lastSequenceToken.get());
					PutLogEventsResult result
							= awsLogsClient.putLogEvents(putLogEventsRequest);
					lastSequenceToken.set(result.getNextSequenceToken());
				} catch (InvalidSequenceTokenException invalidSequenceTokenException) {
					System.err.println("Resetting sequenceToken");
					putLogEventsRequest
							.setSequenceToken(invalidSequenceTokenException
									.getExpectedSequenceToken());
					PutLogEventsResult result
							= awsLogsClient.putLogEvents(putLogEventsRequest);
					lastSequenceToken.set(result.getNextSequenceToken());
				}
			}
		} catch (Exception e) {
			// should never happen
			System.err.println("IGNORED: " + e.toString());
			e.printStackTrace();
		}
	}

	protected synchronized void initializeBackgroundThreads() {
		exe = new ScheduledThreadPoolExecutor(1);
		exe.scheduleAtFixedRate(() -> {
			if (loggingEventsQueue.size() > 0) {
				sendMessages();
			}
		}, 0, 1, TimeUnit.SECONDS);
	}

	public synchronized void initializeCloudwatchResources(String logGroupName, String logStreamName) {
		
		this.logGroupName = logGroupName;
		this.logStreamName = logStreamName;
		
		DescribeLogGroupsRequest describeLogGroupsRequest
				= new DescribeLogGroupsRequest();
		describeLogGroupsRequest.setLogGroupNamePrefix(logGroupName);
		Optional<LogGroup> logGroupOptional
				= awsLogsClient.describeLogGroups(describeLogGroupsRequest)
						.getLogGroups().stream().filter(logGroup -> logGroup
								.getLogGroupName().equals(logGroupName))
						.findFirst();
		if (!logGroupOptional.isPresent()) {
			CreateLogGroupRequest createLogGroupRequest
					= new CreateLogGroupRequest()
							.withLogGroupName(logGroupName);
			awsLogsClient.createLogGroup(createLogGroupRequest);
		}
		DescribeLogStreamsRequest describeLogStreamsRequest
				= new DescribeLogStreamsRequest().withLogGroupName(logGroupName)
						.withLogStreamNamePrefix(logStreamName);
		Optional<LogStream> logStreamOptional
				= awsLogsClient.describeLogStreams(describeLogStreamsRequest)
						.getLogStreams().stream().filter(logStream -> logStream
								.getLogStreamName().equals(logStreamName))
						.findFirst();
		if (!logStreamOptional.isPresent()) {
			System.out.println("About to create LogStream: " + logStreamName
					+ " in LogGroup: " + logGroupName);
			CreateLogStreamRequest createLogStreamRequest
					= new CreateLogStreamRequest()
							.withLogGroupName(logGroupName)
							.withLogStreamName(logStreamName);
			awsLogsClient.createLogStream(createLogStreamRequest);
		}
	}

	public synchronized void publish(LogRecord record) {
		loggingEventsQueue.add(record);
	}

	private synchronized void init(String logGroupName, String logStreamName) throws IOException {
		System.err.println(
				"Initializing CloudwatchAppender with LogGroupName("
						+ logGroupName + ") and LogStreamName("
						+ logStreamName + ")");
			
		this.awsLogsClient = AWSLogsClientBuilder.standard()
				.withRegion(new DefaultAwsRegionProviderChain().getRegion())
				.build();

		initializeCloudwatchResources(logGroupName, logStreamName);
		
		initializeBackgroundThreads();
		
	}

	public synchronized void close() throws SecurityException {
		exe.shutdown();
		try {
			exe.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			exe.shutdownNow();
		}
		finally {
			flush();
		}
	}

	public synchronized void flush() {
		sendMessages();
	}
}
