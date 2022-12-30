/* Copyright 2022 Listware */

package org.listware.io.utils;

import org.apache.flink.statefun.sdk.kafka.KafkaIngressDeserializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.listware.sdk.Functions.FunctionContext;

import com.google.protobuf.ByteString;

public class TypedValueDeserializer implements KafkaIngressDeserializer<TypedValue> {
	private static final long serialVersionUID = 1L;

	@Override
	public TypedValue deserialize(ConsumerRecord<byte[], byte[]> input) {
		return TypedValue.newBuilder().setValue(ByteString.copyFrom(input.value())).setHasValue(true).build();
	}

	public static TypedValue fromMessageLite(FunctionContext functionContext) {
		return TypedValue.newBuilder().setValue(functionContext.toByteString()).setHasValue(true).build();
	}
}
