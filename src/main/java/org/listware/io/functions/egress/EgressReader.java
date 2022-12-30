/* Copyright 2022 Listware */

package org.listware.io.functions.egress;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.listware.sdk.Functions;
import org.listware.io.utils.Constants;
import org.listware.io.utils.KafkaTypedValueDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EgressReader {
	@SuppressWarnings("unused")
	private final Logger LOG = LoggerFactory.getLogger(EgressReader.class);

	private Properties properties = new Properties();
	private KafkaConsumer<String, TypedValue> consumer;
	private String topic;
	private Duration duration = Duration.ofSeconds(5);

	public EgressReader(String groupID, String topic) {
		this.topic = topic;

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.Kafka.SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				KafkaTypedValueDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
		consumer = new KafkaConsumer<String, TypedValue>(properties);
		TopicPartition tp = new TopicPartition(topic, 0);
		List<TopicPartition> tps = Arrays.asList(tp);
		consumer.assign(tps);
		consumer.seekToEnd(tps);
	}

	public void wait(String key) throws Exception {
		long startTime = System.currentTimeMillis();

		LOG.info("ReplyEgress wait: " + key);

		for (;;) {
			long endTime = System.currentTimeMillis();

			if (endTime - startTime >= duration.getSeconds())
				break;

			ConsumerRecords<String, TypedValue> records = consumer.poll(Duration.ofMillis(500));

			for (ConsumerRecord<String, TypedValue> record : records) {
				if (record.key().equals(key)) {
					TypedValue typedValue = record.value();
					Functions.FunctionResult functionResult = Functions.FunctionResult
							.parseFrom(typedValue.toByteArray());
					if (!functionResult.getComplete()) {
						throw new ResultException(functionResult.getError());
					}
					return;
				}
			}
		}

		throw new KeyNotFoundException(key);
	}

	public Functions.ReplyEgress replyEgress() {
		UUID uuid = UUID.randomUUID();
		LOG.info("ReplyEgress new: " + uuid.toString());
		return Functions.ReplyEgress.newBuilder().setNamespace(Constants.Namespaces.INTERNAL).setTopic(topic)
				.setId(uuid.toString()).build();
	}

	public class KeyNotFoundException extends Exception {
		private static final long serialVersionUID = 1L;

		public KeyNotFoundException(String key) {
			super(String.format("key '%s' not found", key));
		}
	}

	public class ResultException extends Exception {
		private static final long serialVersionUID = 1L;

		public ResultException(String reason) {
			super(reason);
		}
	}

}
