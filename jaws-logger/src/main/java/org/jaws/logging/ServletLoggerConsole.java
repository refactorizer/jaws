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

public class ServletLoggerConsole implements Filter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		System.out.println("LOADED: " + this.getClass());
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		long started = System.currentTimeMillis(); 

		chain.doFilter(request, response);

		long elapsed = System.currentTimeMillis() - started;
		
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		HttpServletResponse httpResponse = (HttpServletResponse) response;
		Optional<HttpSession> session = Optional.ofNullable(httpRequest.getSession(false));
		ServletLogEntry e = new ServletLogEntry(httpRequest, httpResponse, session, elapsed);
		
		System.out.println(e.toString());
	}

	@Override
	public void destroy() {
	}
}
