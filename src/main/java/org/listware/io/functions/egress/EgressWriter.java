/* Copyright 2022 Listware */

package org.listware.io.functions.egress;

import java.util.Properties;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.listware.io.utils.Constants;
import org.listware.io.utils.KafkaTypedValueSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EgressWriter {
	@SuppressWarnings("unused")
	private final Logger LOG = LoggerFactory.getLogger(EgressWriter.class);

	private Properties properties = new Properties();
	private KafkaProducer<String, TypedValue> producer;

	public EgressWriter() {
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.Kafka.SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaTypedValueSerializer.class.getName());

		producer = new KafkaProducer<String, TypedValue>(properties);
	}

	public void Send(String topic, String key, TypedValue typedValue) {
		ProducerRecord<String, TypedValue> producerRecord = new ProducerRecord<>(topic, key, typedValue);
		producer.send(producerRecord);
	}

}
