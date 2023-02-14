/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.io.utils;

import java.nio.charset.StandardCharsets;

import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.InvalidProtocolBufferException;

public class KafkaEgressTypedValueSerializer implements KafkaEgressSerializer<TypedValue> {
	private final Logger LOG = LoggerFactory.getLogger(KafkaEgressTypedValueSerializer.class);

	private static final long serialVersionUID = 1L;

	@Override
	public ProducerRecord<byte[], byte[]> serialize(TypedValue message) {
		// TODO errors egress?
		String topic = "default";
		String key = "default";
		byte[] value = "default".getBytes();

		try {
			KafkaProducerRecord kafkaProducerRecord = KafkaProducerRecord.parseFrom(message.getValue());
			topic = kafkaProducerRecord.getTopic();
			key = kafkaProducerRecord.getKey();
			value = message.toByteArray();
		} catch (InvalidProtocolBufferException e) {
			LOG.error(e.getLocalizedMessage());
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
		}
		return new ProducerRecord<byte[], byte[]>(topic, key.getBytes(StandardCharsets.UTF_8), value);
	}

}