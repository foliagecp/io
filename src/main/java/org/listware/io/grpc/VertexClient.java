/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.io.grpc;

import java.util.concurrent.TimeUnit;

import org.listware.sdk.pbcmdb.Core.Request;
import org.listware.sdk.pbcmdb.Core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.listware.io.utils.Constants.Cmdb;
import org.listware.sdk.pbcmdb.VertexServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

// interface over database, in future will be not only 'arangodb'
public class VertexClient {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(VertexClient.class);

	private final ManagedChannel channel;
	private final VertexServiceGrpc.VertexServiceBlockingStub blockingStub;

	public VertexClient() {
		this(ManagedChannelBuilder.forAddress(Cmdb.Addr(), Cmdb.Port()).usePlaintext().build());
	}

	public VertexClient(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = VertexServiceGrpc.newBlockingStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public Response create(String collection, ByteString payload) throws Exception {
		Request request = Request.newBuilder().setCollection(collection).setPayload(payload).build();
		return blockingStub.create(request);
	}

	public Response read(String collection, String key) throws Exception {
		Request request = Request.newBuilder().setCollection(collection).setKey(key).build();
		return blockingStub.read(request);
	}

	public Response update(String collection, String key, ByteString payload) throws Exception {
		Request request = Request.newBuilder().setCollection(collection).setKey(key).setPayload(payload).build();
		return blockingStub.update(request);
	}

	public Response remove(String collection, String key) throws Exception {
		Request request = Request.newBuilder().setCollection(collection).setKey(key).build();
		return blockingStub.remove(request);
	}
}
