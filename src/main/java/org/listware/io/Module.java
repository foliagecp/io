/*
 *  Copyright 2023 NJWS Inc.
 *  Copyright 2022 Listware
 */

package org.listware.io;

import java.util.Map;

import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.listware.io.functions.result.Egress;
import org.listware.io.router.IngressRouter;
import org.listware.io.server.HealthServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(StatefulFunctionModule.class)
public class Module implements StatefulFunctionModule {
	private static final Logger LOG = LoggerFactory.getLogger(Module.class);

	private IngressRouter ingressRouter = new IngressRouter();
	private HealthServer healthServer = new HealthServer();

	@Override
	public void configure(Map<String, String> globalConfiguration, Binder binder) {
		binder.bindIngress(IngressRouter.INGRESS_SPEC);
		binder.bindIngressRouter(IngressRouter.INGRESS, ingressRouter);
		binder.bindEgress(Egress.EGRESS_SPEC);
		health();
	}

	private void health() {
		try {
			healthServer.start();
			LOG.debug("health server started");
		} catch (Exception e) {
			LOG.error(e.getLocalizedMessage());
		}
	}

}
