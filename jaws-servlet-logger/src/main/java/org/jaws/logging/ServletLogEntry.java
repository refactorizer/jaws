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
	final public String referer;
	final public String user_agent;
	final public long time_elapsed;
	final public String session_id;
	final public Long user_id;
	final public int agent_proxy;
	final public String agent_id;
	final public String thread_name;
	final public String host;
	final public String pagename;
	final public String listing_id;

	public ServletLogEntry(HttpServletRequest httpRequest, HttpServletResponse httpResponse, Optional<HttpSession> session, long elapsed) {
		this.server_ts = Instant.now();
		
		String xff = httpRequest.getHeader("x-forwarded-for");
		if(xff != null) {
			this.remote_ip = xff.split(",")[0];
		}
		else {
			this.remote_ip = httpRequest.getRemoteAddr();
		}
		
		this.local_ip = httpRequest.getLocalAddr();
		this.method = httpRequest.getMethod();
		this.url = httpRequest.getPathInfo();
		this.query_string = Optional.ofNullable(httpRequest.getQueryString()).filter(s -> s != null && !s.isEmpty())
							.map(qs -> '?' + httpRequest.getQueryString()).orElse(null);
		this.protocol = httpRequest.getProtocol();
		this.http_status = httpResponse.getStatus();
		this.referer = httpRequest.getHeader("referer");
		this.user_agent = httpRequest.getHeader("user-agent");
		this.time_elapsed = elapsed;
		this.session_id = session.map(HttpSession::getId).orElse(null);
		this.thread_name = Thread.currentThread().getName();
		this.host = httpRequest.getHeader("host");

		/**
		 * custom attributes
		 */
		this.user_id = (Long) session.map(s -> s.getAttribute("user_id")).orElse(null);
		this.agent_proxy = (Integer) session.map(s -> s.getAttribute("agent_proxy")).orElse(0);
		this.agent_id = ServletLogEntry.getCookie(httpRequest, "agent_device_id")
								.map(Cookie::getValue).orElse(null);
		this.pagename = (String) httpRequest.getAttribute("pagename");
		this.listing_id = (String) httpRequest.getAttribute("listing_id");
				
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


	@Override
	public String toString() {
		return "ServletLogEntry [server_ts=" + server_ts + ", remote_ip=" + remote_ip + ", local_ip=" + local_ip
				+ ", method=" + method + ", url=" + url + ", query_string=" + query_string + ", protocol=" + protocol
				+ ", http_status=" + http_status + ", referer=" + referer + ", user_agent=" + user_agent
				+ ", time_elapsed=" + time_elapsed + ", session_id=" + session_id + ", user_id=" + user_id
				+ ", agent_proxy=" + agent_proxy + ", agent_id=" + agent_id + ", thread_name=" + thread_name + ", host="
				+ host + ", pagename=" + pagename + ", listing_id=" + listing_id + "]";
	}

}