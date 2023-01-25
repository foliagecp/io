/* Copyright 2022 Listware */

package org.listware.io.functions.result;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.listware.io.utils.Constants;
import org.listware.io.utils.KafkaTypedValueDeserializer;
import org.listware.sdk.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EgressReader {
	@SuppressWarnings("unused")
	private final Logger LOG = LoggerFactory.getLogger(EgressReader.class);

	private Properties properties = new Properties();
	private KafkaConsumer<String, TypedValue> consumer;
	private String topic;
	// private Duration duration = Duration.ofSeconds(5);

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

	public Result.ReplyResult replyResult(String id) {
		UUID uuid = UUID.randomUUID();

		Result.ReplyResult replyEgress = Result.ReplyResult.newBuilder().setKey(uuid.toString())
				.setNamespace(Constants.Namespaces.INTERNAL).setTopic(topic).setId(id).build();

		return replyEgress;
	}

	public static class ReplyResult {
		private String key;
		private String id;
		private String namespace;
		private String topic;
		private Boolean isEgress;

		public ReplyResult() {
			// POJO
		}

		public ReplyResult(Result.ReplyResult replyResult) {
			super();
			this.id = replyResult.getId();
			this.key = replyResult.getKey();
			this.namespace = replyResult.getNamespace();
			this.topic = replyResult.getTopic();
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getNamespace() {
			return namespace;
		}

		public void setNamespace(String namespace) {
			this.namespace = namespace;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public Boolean getIsEgress() {
			return isEgress;
		}

		public void setIsEgress(Boolean isEgress) {
			this.isEgress = isEgress;
		}

		public Result.ReplyResult toProto() {
			return Result.ReplyResult.newBuilder().setId(id).setNamespace(namespace).setKey(key).setTopic(topic)
					.build();
		}

		public Address toAddress() {
			FunctionType functionType = new FunctionType(getNamespace(), getTopic());
			Address address = new Address(functionType, getId());
			return address;
		}
	}

	public static class KeyNotFoundException extends Exception {
		private static final long serialVersionUID = 1L;

		public KeyNotFoundException(String key) {
			// TODO rename to timeout error
			super(String.format("key '%s' not found", key));
		}
	}

	public static class ResultException extends Exception {
		private static final long serialVersionUID = 1L;

		public ResultException(String reason) {
			super(reason);
		}
	}

}
