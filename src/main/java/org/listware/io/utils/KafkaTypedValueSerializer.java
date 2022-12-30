/* Copyright 2022 Listware */

package org.listware.io.utils;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaTypedValueSerializer implements Serializer<TypedValue>  {

	@Override
	public byte[] serialize(String topic, TypedValue data) {
		return data.toByteArray();
	}

}
