/* Copyright 2022 Listware */

package org.listware.io.utils;

import java.nio.charset.StandardCharsets;

import org.apache.flink.statefun.sdk.kafka.KafkaEgressSerializer;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.listware.sdk.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class KafkaEgressTypedValueSerializer implements KafkaEgressSerializer<TypedValue> {
	private final Logger LOG = LoggerFactory.getLogger(KafkaEgressTypedValueSerializer.class);

	private static final long serialVersionUID = 1L;

	@Override
	public ProducerRecord<byte[], byte[]> serialize(TypedValue message) {
		try {
			Functions.FunctionResult functionResult = Functions.FunctionResult.parseFrom(message.getValue());

			Functions.ReplyEgress replyEgress = functionResult.getReplyEgress();

			String topic = replyEgress.getTopic();

			byte[] key = replyEgress.getId().getBytes(StandardCharsets.UTF_8);
			byte[] value = message.toByteArray();

			return new ProducerRecord<byte[], byte[]>(topic, key, value);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(e.getLocalizedMessage());
		}

		return null;
	}

}