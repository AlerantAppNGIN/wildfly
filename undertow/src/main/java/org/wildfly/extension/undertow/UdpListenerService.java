package org.wildfly.extension.undertow;

import io.undertow.server.ListenerRegistry;
import io.undertow.server.OpenListener;
import io.undertow.server.handlers.udp.RootUdpHandler;
import io.undertow.server.protocol.udp.UdpOpenListener;
import io.undertow.server.protocol.udp.UdpReadListener;
import io.undertow.server.protocol.udp.UdpWriteListener;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.value.InjectedValue;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.MulticastMessageChannel;

public class UdpListenerService extends ListenerService<UdpListenerService> {
    private volatile MulticastMessageChannel server;
    private RootUdpHandler rootHandler;

    protected final InjectedValue<ListenerRegistry> udpListenerRegistry = new InjectedValue<>();
    static final String PROTOCOL = "udp";

    private final String serverName;

    protected volatile UdpOpenListener udpOpenListener;

    public UdpListenerService(String name, final String serverName, OptionMap listenerOptions, OptionMap socketOptions,
            boolean certificateForwarding, boolean proxyAddressForwarding) {
        super(name, listenerOptions, socketOptions);
        this.serverName = serverName;
    }

    @Override
    protected OpenListener createOpenListener() {
        // UdpOpenListener required instead of this
        return null;
    }

    protected UdpOpenListener createOpenListerner() {
        return new UdpOpenListener();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public UdpListenerService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    @Override
    protected void preStart(final StartContext context) {
        // adds the HTTP upgrade service
        // TODO: have a bit more of a think about how we handle this
        ListenerRegistry.Listener listener = new ListenerRegistry.Listener(getProtocol(), getName(), serverName,
                getBinding().getValue().getSocketAddress());
        listener.setContextInformation("socket-binding", getBinding().getValue());
        udpListenerRegistry.getValue().addListener(listener);
    }

    @Override
    public void start(StartContext context) throws StartException {
        preStart(context);
        serverService.getValue().registerListener(this);
        try {
            final InetSocketAddress socketAddress = binding.getValue().getSocketAddress();
            udpOpenListener = createOpenListerner();

            rootHandler = serverService.getValue().getRootUdpHandler();

            /*
             * TODO:for(HandlerWrapper wrapper : listenerHandlerWrappers) { handler = wrapper.wrap(handler); }
             */

            udpOpenListener.setRootHandler(rootHandler);
            startListening(worker.getValue(), socketAddress, null);
            registerBinding();
        } catch (IOException e) {
            throw new StartException("Could not start udp listener", e);
        }
    }

    @Override
    void startListening(XnioWorker worker, InetSocketAddress socketAddress,
            ChannelListener<AcceptingChannel<StreamConnection>> acceptListener) throws IOException {

        final int MAX_RECEIVE_BUFFER_SIZE = 65535;
        final int MAX_SEND_BUFFER_SIZE = 65535;
        OptionMap udpSocketOptions = OptionMap.builder().set(Options.MULTICAST, false)
                .set(Options.RECEIVE_BUFFER, MAX_RECEIVE_BUFFER_SIZE).set(Options.SEND_BUFFER, MAX_SEND_BUFFER_SIZE)
                .addAll(socketOptions).getMap();

        server = worker.createUdpServer(socketAddress, udpOpenListener, udpSocketOptions);

        UdpReadListener readListerner = new UdpReadListener(rootHandler, MAX_RECEIVE_BUFFER_SIZE);
        UdpWriteListener writeListerner = new UdpWriteListener();

        server.getReadSetter().set(readListerner);
        server.getWriteSetter().set(writeListerner);

        server.resumeReads();

        UndertowLogger.ROOT_LOGGER.listenerStarted(PROTOCOL, getName(), socketAddress);

    }

    @Override
    void stopListening() {
        if(server!=null){
            server.suspendReads();
            server.suspendWrites();
        }

        UndertowLogger.ROOT_LOGGER.listenerSuspend(PROTOCOL, getName());
        IoUtils.safeClose(server);
        server = null;
        UndertowLogger.ROOT_LOGGER.listenerStopped(PROTOCOL, getName(), getBinding().getValue().getSocketAddress());
        udpListenerRegistry.getValue().removeListener(getName());
    }

    @Override
    protected String getProtocol() {
        return PROTOCOL;
    }

    public InjectedValue<ListenerRegistry> getUdpListenerRegistry() {
        return udpListenerRegistry;
    }

}
