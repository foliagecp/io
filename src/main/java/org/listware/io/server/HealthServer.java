/*
 *  Copyright 2023 NJWS Inc.
 */

package org.listware.io.server;

import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

public class HealthServer {
	private HttpServer server = null;

	public HealthServer() {
	}

	public void start() throws Exception {
		server = HttpServer.create();
		server.bind(new InetSocketAddress(51000), 0);
		server.createContext("/readyz", new Handler());
		server.createContext("/livez", new Handler());
		server.start();
	}
}
