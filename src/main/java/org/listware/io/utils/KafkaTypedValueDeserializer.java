/* Copyright 2022 Listware */

package org.listware.io.utils;

import java.util.Map;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class KafkaTypedValueDeserializer implements Deserializer<TypedValue> {
	private final Logger LOG = LoggerFactory.getLogger(KafkaTypedValueDeserializer.class);

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public TypedValue deserialize(String topic, byte[] data) {
		try {
			return TypedValue.parseFrom(data);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(e.getLocalizedMessage());
		}
		return null;
	}
}
