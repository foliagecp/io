/* Copyright 2022 Listware */

package org.listware.io.utils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

import org.listware.sdk.pbcmdb.pbqdsl.QDSL;
import org.listware.sdk.pbcmdb.pbqdsl.QdslServiceGrpc;
import org.listware.io.utils.Constants.Cmdb.Qdsl;

public class QDSLClient {
	private final ManagedChannel channel;
	private final QdslServiceGrpc.QdslServiceBlockingStub blockingStub;

	public QDSLClient() {
		this(ManagedChannelBuilder.forAddress(Qdsl.ADDR, Qdsl.PORT).usePlaintext().build());
	}

	public QDSLClient(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = QdslServiceGrpc.newBlockingStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public QDSL.Elements qdsl(String query, QDSL.Options options) throws StatusRuntimeException {
		QDSL.Query request = QDSL.Query.newBuilder().setQuery(query).setOptions(options).build();
		return blockingStub.qdsl(request);
	}
}
