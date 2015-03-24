/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.wildfly.extension.undertow;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.List;

import io.undertow.server.HandlerWrapper;
import io.undertow.server.HttpHandler;
import io.undertow.server.ListenerRegistry;
import io.undertow.server.handlers.PeerNameResolvingHandler;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.network.SocketBinding;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.wildfly.extension.io.IOServices;
import org.wildfly.extension.io.OptionList;
import org.xnio.OptionMap;
import org.xnio.Pool;
import org.xnio.XnioWorker;

/**
 * @author <a href="mailto:tomaz.cerar@redhat.com">Tomaz Cerar</a> (c) 2012 Red Hat Inc.
 */
public class UdpListenerAdd extends ListenerAdd {

    static final ServiceName REGISTRY_SERVICE_NAME = ServiceName.JBOSS.append("http", "listener", "registry");

    UdpListenerAdd(ListenerResourceDefinition definition) {
        super(definition);
    }


    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.get(OP_ADDR));
        final PathAddress parent = address.subAddress(0, address.size() - 1);
        String name = address.getLastElement().getValue();
        String bindingRef = ListenerResourceDefinition.SOCKET_BINDING.resolveModelAttribute(context, model).asString();
        String workerName = ListenerResourceDefinition.WORKER.resolveModelAttribute(context, model).asString();
        String bufferPoolName = ListenerResourceDefinition.BUFFER_POOL.resolveModelAttribute(context, model).asString();
        final boolean enabled = ListenerResourceDefinition.ENABLED.resolveModelAttribute(context, model).asBoolean();
        final boolean peerHostLookup = ListenerResourceDefinition.RESOLVE_PEER_ADDRESS.resolveModelAttribute(context, model).asBoolean();

        OptionMap listenerOptions = OptionList.resolveOptions(context, model, ListenerResourceDefinition.LISTENER_OPTIONS);
        OptionMap socketOptions = OptionList.resolveOptions(context, model, ListenerResourceDefinition.SOCKET_OPTIONS);
        String serverName = parent.getLastElement().getValue();
        final ServiceName listenerServiceName = UndertowService.listenerName(name);
        final ListenerService<? extends ListenerService> service = createService(name, serverName, context, model, listenerOptions,socketOptions);
        if (peerHostLookup) {
            service.addWrapperHandler(new HandlerWrapper() {
                @Override
                public HttpHandler wrap(HttpHandler handler) {
                    return new PeerNameResolvingHandler(handler);
                }
            });
        }

        final ServiceBuilder<? extends ListenerService> serviceBuilder = context.getServiceTarget().addService(listenerServiceName, service);
        serviceBuilder.addDependency(IOServices.WORKER.append(workerName), XnioWorker.class, service.getWorker())
                .addDependency(SocketBinding.JBOSS_BINDING_NAME.append(bindingRef), SocketBinding.class, service.getBinding())
                .addDependency(IOServices.BUFFER_POOL.append(bufferPoolName), Pool.class, service.getBufferPool())
                .addDependency(UndertowService.SERVER.append(serverName), Server.class, service.getServerService());

        configureAdditionalDependencies(context, serviceBuilder, model, service);
        serviceBuilder.setInitialMode(enabled ? ServiceController.Mode.ON_DEMAND : ServiceController.Mode.NEVER);

        serviceBuilder.addListener(verificationHandler);
        final ServiceController<? extends ListenerService> serviceController = serviceBuilder.install();
        if (newControllers != null) {
            newControllers.add(serviceController);
        }
    }

    @Override
    ListenerService<? extends ListenerService> createService(String name, final String serverName, final OperationContext context, ModelNode model, OptionMap listenerOptions, OptionMap socketOptions) throws OperationFailedException {
        return new UdpListenerService(name, serverName, listenerOptions, socketOptions, false, false);
    }

    @Override
    void configureAdditionalDependencies(OperationContext context, ServiceBuilder<? extends ListenerService> serviceBuilder, ModelNode model, ListenerService service) throws OperationFailedException {
        String redirectBindingRef = ListenerResourceDefinition.REDIRECT_SOCKET.resolveModelAttribute(context, model).asString();
        serviceBuilder.addDependency(SocketBinding.JBOSS_BINDING_NAME.append(redirectBindingRef), SocketBinding.class, service.getRedirectSocket());
        serviceBuilder.addDependency(REGISTRY_SERVICE_NAME, ListenerRegistry.class, ((UdpListenerService) service).getUdpListenerRegistry());
    }
}
