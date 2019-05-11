package org.jaws.logging;

import java.time.Instant;
import java.util.Optional;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.fasterxml.jackson.annotation.JsonGetter;

public class ServletLogEntry {
	final public Instant server_ts;
	@JsonGetter("server_ts")
	public String getServerTs() {
		return server_ts.toString();
	}
	final public String remote_ip;
	final public String local_ip;
	final public String method;
	final public String url;
	final public String query_string;
	final public String protocol;
	final public int http_status;
	final public Integer bytes_sent;
	final public String referer;
	final public String user_agent;
	final public long time_elapsed;
	final public String session_id;
	final public Long user_id;
	final public Integer agent_proxy;
	final public String agent_id;
	final public Integer time_to_first_byte;
	final public String thread_name;
	final public String host;

	public ServletLogEntry(Instant server_ts, String remote_ip, String local_ip, String method, String url,
			String query_string, String protocol, int http_status, Integer bytes_sent, String referer,
			String user_agent, long time_elapsed, String session_id, Long user_id, Integer agent_proxy, String agent_id,
			Integer time_to_first_byte, String thread_name, String host) {
		this.server_ts = server_ts;
		this.remote_ip = remote_ip;
		this.local_ip = local_ip;
		this.method = method;
		this.url = url;
		this.query_string = query_string;
		this.protocol = protocol;
		this.http_status = http_status;
		this.bytes_sent = bytes_sent;
		this.referer = referer;
		this.user_agent = user_agent;
		this.time_elapsed = time_elapsed;
		this.session_id = session_id;
		this.user_id = user_id;
		this.agent_proxy = agent_proxy;
		this.agent_id = agent_id;
		this.time_to_first_byte = time_to_first_byte;
		this.thread_name = thread_name;
		this.host = host;
	}
	

	public ServletLogEntry(HttpServletRequest httpRequest, HttpServletResponse httpResponse, Optional<HttpSession> session, long elapsed) {
		this(Instant.now(), httpRequest.getRemoteAddr(), httpRequest.getLocalAddr(), httpRequest.getMethod(), httpRequest.getPathInfo(),
				Optional.ofNullable(httpRequest.getQueryString()).filter(s -> s != null && !s.isEmpty())
					.map(qs -> '?' + httpRequest.getQueryString()).orElse(null), httpRequest.getProtocol(), httpResponse.getStatus(), 0, httpRequest.getHeader("referer"), httpRequest.getHeader("user-agent"), elapsed,
				session.map(HttpSession::getId).orElse(null), (Long) session.map(s -> s.getAttribute("user_id")).orElse(null), (Integer) session.map(s -> s.getAttribute("agent_proxy")).orElse(null), ServletLogEntry.getCookie(httpRequest, "agent_device_id")
						.map(Cookie::getValue).orElse(null), 0, Thread.currentThread().getName(), httpRequest.getHeader("host"));
	}


	private static Optional<Cookie> getCookie(HttpServletRequest httpRequest, String name) {
		Cookie[] cookies = httpRequest.getCookies();

		if (cookies != null) {
		 for (Cookie cookie : cookies) {
		   if (cookie.getName().equals(name)) {
			   return Optional.of(cookie);
		    }
		  }
		}
		return Optional.empty();
	}

}