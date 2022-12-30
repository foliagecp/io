/* Copyright 2022 Listware */

package org.listware.io.router;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.kafka.KafkaIngressBuilder;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.listware.sdk.Functions.FunctionContext;
import org.listware.io.utils.Constants;
import org.listware.io.utils.TypedValueDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class IngressRouter implements Router<TypedValue> {
	private static final Logger LOG = LoggerFactory.getLogger(IngressRouter.class);

	private static final String INGRESS_TOPIC_NAME = "router.system";

	private static final String GROUP_ID = "group.system";

	public static final IngressIdentifier<TypedValue> INGRESS = new IngressIdentifier<>(TypedValue.class,
			Constants.Namespaces.INTERNAL, INGRESS_TOPIC_NAME);

	public static IngressSpec<TypedValue> INGRESS_SPEC = KafkaIngressBuilder.forIdentifier(INGRESS)
			.withProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID).withKafkaAddress(Constants.Kafka.SERVER)
			.withTopic(INGRESS_TOPIC_NAME).withDeserializer(TypedValueDeserializer.class).build();

	@Override
	public void route(TypedValue message, Downstream<TypedValue> downstream) {

		try {
			FunctionContext functionContext = FunctionContext.parseFrom(message.getValue());

			String namespace = functionContext.getFunctionType().getNamespace();
			String type = functionContext.getFunctionType().getType();

			FunctionType functionType = new FunctionType(namespace, type);

			downstream.forward(functionType, functionContext.getId(), message);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(e.getLocalizedMessage());
		}
	}

}
