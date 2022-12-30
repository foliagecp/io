/* Copyright 2022 Listware */

package org.listware.io.utils;

import java.util.concurrent.TimeUnit;

import org.listware.sdk.pbcmdb.Core.Request;
import org.listware.sdk.pbcmdb.Core.Response;
import org.listware.io.utils.Constants.Cmdb.Qdsl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.listware.sdk.pbcmdb.VertexServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class VertexClient {
	private static final Logger LOG = LoggerFactory.getLogger(VertexClient.class);

	private final ManagedChannel channel;
	private final VertexServiceGrpc.VertexServiceBlockingStub blockingStub;

	public VertexClient() {
		this(ManagedChannelBuilder.forAddress(Qdsl.ADDR, Qdsl.PORT).usePlaintext().build());
	}

	public VertexClient(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = VertexServiceGrpc.newBlockingStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public Response read(String key, String collection) throws StatusRuntimeException {
		LOG.info("read " + key + " from " + collection);
		Request request = Request.newBuilder().setCollection(collection).setKey(key).build();
		Response resp = blockingStub.read(request);
		return resp;
	}

	public Response remove(String key, String collection) throws StatusRuntimeException {
		Request request = Request.newBuilder().setCollection(collection).setKey(key).build();
		return blockingStub.remove(request);
	}
}
