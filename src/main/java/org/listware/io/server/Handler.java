/*
 *  Copyright 2023 NJWS Inc.
 */

package org.listware.io.server;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class Handler implements HttpHandler {

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		byte[] response = "OK".getBytes();
		exchange.sendResponseHeaders(200, response.length);
		OutputStream os = exchange.getResponseBody();
		os.write(response);
		os.close();
	}

}
