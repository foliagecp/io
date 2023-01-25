package org.listware.io.grpc;

import java.util.concurrent.TimeUnit;

import org.listware.io.utils.Constants.Cmdb;
import org.listware.sdk.pbcmdb.pbfinder.Finder;
import org.listware.sdk.pbcmdb.pbfinder.FinderServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class FinderClient {
	private final ManagedChannel channel;
	private final FinderServiceGrpc.FinderServiceBlockingStub blockingStub;

	public FinderClient() {
		this(ManagedChannelBuilder.forAddress(Cmdb.ADDR, Cmdb.PORT).usePlaintext().build());
	}

	public FinderClient(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = FinderServiceGrpc.newBlockingStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	public Finder.Response from(String from) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setFrom(from).build();
		return blockingStub.links(request);
	}

	public Finder.Response from(String from, String name) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setFrom(from).setName(name).build();
		return blockingStub.links(request);
	}

	public Finder.Response to(String to) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setTo(to).build();
		return blockingStub.links(request);
	}

	public Finder.Response to(String to, String name) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setTo(to).setName(name).build();
		return blockingStub.links(request);
	}

	public Finder.Response all(String from, String to) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setFrom(from).setTo(to).build();
		return blockingStub.links(request);
	}

	public Finder.Response all(String from, String to, String name) throws StatusRuntimeException {
		Finder.Request request = Finder.Request.newBuilder().setFrom(from).setTo(to).setName(name).build();
		return blockingStub.links(request);
	}
}
