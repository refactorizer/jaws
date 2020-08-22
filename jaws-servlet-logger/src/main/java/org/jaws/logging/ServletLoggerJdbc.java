package org.jaws.logging;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ServletLoggerJdbc implements Filter {

	private String dbUrl = null;
	private String driver;
	private String user;
	private String password;
	
	private List<ServletLogEntry> queue = new LinkedList<ServletLogEntry>();

	private final String sqlStatement = "insert into log_access (server_ts,remote_ip,local_ip,method,url,query_string,protocol,http_status,referer,user_agent,time_elapsed,session_id,user_id,agent_proxy,agent_id,thread_name,host,pagename,listing_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		try {

			Properties p = new Properties();
			String path = System.getProperty("ctc_config_path");
			URL propertiesUrl = new URL(path + "/tomcat_db_logging.properties");
			InputStream is = propertiesUrl.openStream();
			p.load(is);
			is.close();

			this.dbUrl = p.getProperty("database.url");
			this.driver = p.getProperty("database.driver.classname");
			this.user = p.getProperty("database.username");
			this.password = p.getProperty("database.password");

			Class.forName(driver);

			try(Connection test = DriverManager.getConnection(dbUrl, user, password)) {
				//empty;
			}

			System.out.println(Instant.now() + " " + this.getClass().getName() + " Connected to database " + dbUrl);

		} catch (IOException | SQLException | ClassNotFoundException e) {
			System.err.println(Instant.now() + " " + this.getClass().getName() + " FAILED TO CONNECT TO DATABASE " + dbUrl);
			e.printStackTrace(System.err);
		}
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		
		long started = System.currentTimeMillis(); 

		chain.doFilter(request, response);
		
		long elapsed = System.currentTimeMillis() - started;

		log(request, response, elapsed);

	}

	protected void log(ServletRequest request, ServletResponse response, long elapsed) {
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		HttpServletResponse httpResponse = (HttpServletResponse) response;
		ServletLogEntry e = new ServletLogEntry(httpRequest, httpResponse, Optional.ofNullable(httpRequest.getSession(false)), elapsed);
		
		synchronized(queue) {
			queue.add(e);
		}
	}

	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	public ServletLoggerJdbc() {
		scheduler.scheduleAtFixedRate(this::flush, 0, 2, TimeUnit.SECONDS);
	}
	
	private void flush() {
		
		List<ServletLogEntry> dequeue = new ArrayList<>();
		
		synchronized(queue) {
			if(!dequeue.addAll(queue)) {
				return;
			}
			queue.clear();
		}
		
		try(Connection connection = DriverManager.getConnection(dbUrl, user, password);
			PreparedStatement statement = connection.prepareStatement(sqlStatement)) {
			
			for(ServletLogEntry e: dequeue) {
				int n = 0;
				statement.setTimestamp(++n, Timestamp.from(e.server_ts), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
				statement.setString(++n, e.remote_ip);
				statement.setString(++n, e.local_ip);
				statement.setString(++n, e.method);
				statement.setString(++n, e.url);
				statement.setString(++n, e.query_string);
				statement.setString(++n, e.protocol);
				statement.setInt(++n, e.http_status);
				statement.setString(++n, e.referer);
				statement.setString(++n, e.user_agent);
				statement.setLong(++n, e.time_elapsed);
				statement.setString(++n, e.session_id);
				
				++n;
				if(e.user_id != null)
					statement.setLong(n, e.user_id);
				else
					statement.setNull(n, Types.BIGINT);
				
				statement.setInt(++n, e.agent_proxy);
				statement.setString(++n, e.agent_id);
				statement.setString(++n, e.thread_name);
				statement.setString(++n, e.host);
				statement.setString(++n, e.pagename);
				statement.setString(++n, e.listing_id);
				
				statement.addBatch();
			}

			int rc[] = statement.executeBatch();
			if(IntStream.of(rc).sum() != rc.length) {
				System.err.println(Instant.now() + ": WARNING: Some records failed to insert! rc = " + Arrays.asList(rc));
			}
			
		} catch (SQLException e1) {
			System.err.println(Instant.now() + ": ERROR: Failed to insert logs into database: " + dequeue.toString());
		}
	}

	@Override
	public void destroy() {
		scheduler.shutdown();
		try {
			scheduler.awaitTermination(3, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.err.println("FAILED to terminate scheduler");
		}
	}

}
