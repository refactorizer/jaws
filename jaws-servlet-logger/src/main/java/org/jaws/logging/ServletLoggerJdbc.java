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
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServletLoggerJdbc implements Filter {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private String dbUrl = null;
	private String driver;
	private String user;
	private String password;

	private final String sqlStatement = "insert into log_access (server_ts,remote_ip,local_ip,method,url,query_string,protocol,http_status,bytes_sent,referer,user_agent,time_elapsed,session_id,user_id,agent_proxy,agent_id,time_to_first_byte,thread_name,host) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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

			System.out.println(new Date() + " " + this.getClass().getName() + " Connected to database " + dbUrl);

		} catch (IOException | SQLException | ClassNotFoundException e) {
			System.err.println(new Date() + " " + this.getClass().getName() + " FAILED TO CONNECT TO DATABASE " + dbUrl);
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
		
		int retries = 3;

		while (--retries >= 0) {

			int n = 0;

			try(Connection connection = DriverManager.getConnection(dbUrl, user, password);
				PreparedStatement statement = connection.prepareStatement(sqlStatement)) {
				ServletLogEntry e = new ServletLogEntry(httpRequest, httpResponse, Optional.ofNullable(httpRequest.getSession(false)), elapsed);
				
				statement.setTimestamp(++n, Timestamp.from(e.server_ts), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
				statement.setString(++n, e.remote_ip);
				statement.setString(++n, e.local_ip);
				statement.setString(++n, e.method);
				statement.setString(++n, e.url);
				statement.setString(++n, e.query_string);
				statement.setString(++n, e.protocol);
				statement.setInt(++n, e.http_status);
				statement.setInt(++n, e.bytes_sent);
				statement.setString(++n, e.referer);
				statement.setString(++n, e.user_agent);
				statement.setLong(++n, e.time_elapsed);
				statement.setString(++n, e.session_id);
				
				++n;
				if(e.user_id != null)
					statement.setLong(n, e.user_id);
				else
					statement.setNull(n, Types.BIGINT);
				
				statement.setInt(++n, e.agent_proxy != null ? e.agent_proxy : 0);
				statement.setString(++n, e.agent_id);
				statement.setInt(++n, e.time_to_first_byte);
				statement.setString(++n, e.thread_name);
				statement.setString(++n, e.host);

				if (statement.executeUpdate() != 1) {
					throw new SQLException("not inserted 1 row");
				}
				return;

			} catch (SQLException e) {
				logger.warn("Failed to log to database! Will retry another " + retries + " times", e);
			}
		}
	}

	@Override
	public void destroy() {
	}

}
