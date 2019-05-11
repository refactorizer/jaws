package org.jaws.logging;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ServletLoggerCloudwatch implements Filter {

	protected CloudwatchClient client;
	
	private ObjectMapper objectMapper = new ObjectMapper();
	
	public ServletLoggerCloudwatch() {
		this(CloudwatchClient.getInstance());
	}

	public ServletLoggerCloudwatch(CloudwatchClient cloudwatchHandler) {
		this.client = cloudwatchHandler;
	}

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
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
		Optional<HttpSession> session = Optional.ofNullable(httpRequest.getSession(false));
		ServletLogEntry e = new ServletLogEntry(httpRequest, httpResponse, session, elapsed);
		ObjectNode json = objectMapper.convertValue(e, ObjectNode.class); 
		client.publish(new JsonLogRecord(json));
	}

	@Override
	public void destroy() {
	}

}
