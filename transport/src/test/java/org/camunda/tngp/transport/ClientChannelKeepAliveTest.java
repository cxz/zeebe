package org.camunda.tngp.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.agrona.DirectBuffer;
import org.camunda.tngp.test.util.TestUtil;
import org.camunda.tngp.transport.TransportBuilder.ThreadingMode;
import org.camunda.tngp.transport.spi.TransportChannelHandler;
import org.camunda.tngp.util.time.ClockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientChannelKeepAliveTest
{
    protected static final long KEEP_ALIVE_PERIOD = 1000;

    protected Transport clientTransport;
    protected Transport serverTransport;

    @Before
    public void setUp()
    {
        clientTransport = Transports.createTransport("client")
            .threadingMode(ThreadingMode.SHARED)
            .channelKeepAlivePeriod(KEEP_ALIVE_PERIOD)
            .build();

        serverTransport = Transports.createTransport("server")
                .threadingMode(ThreadingMode.SHARED)
                .build();
    }

    @After
    public void tearDown()
    {
        clientTransport.close();
        serverTransport.close();
        ClockUtil.reset();
    }

    @Test
    public void shouldSendInitialKeepAlive() throws InterruptedException
    {
        // given
        ClockUtil.setCurrentTime(Instant.now());

        final SocketAddress addr = new SocketAddress("localhost", 51115);

        final KeepAliveFrameCounter controlFrameHandler = new KeepAliveFrameCounter();
        serverTransport.createServerSocketBinding(addr)
            .transportChannelHandler(controlFrameHandler)
            .bind();

        final ClientChannelPool channelPool = clientTransport.createClientChannelPool().build();

        final ClientChannel channel = channelPool.requestChannel(addr);
        TestUtil.waitUntil(() -> channel.isOpen());

        // when
        ClockUtil.addTime(Duration.ofMillis(KEEP_ALIVE_PERIOD + 1));

        // then
        TestUtil.waitUntil(() -> controlFrameHandler.numReceivedKeepAliveFrames > 0);
        assertThat(controlFrameHandler.numReceivedKeepAliveFrames).isEqualTo(1);
    }

    @Test
    public void shouldSendMoreKeepAliveMessagesAsTimeAdvances()
    {
        // given
        ClockUtil.setCurrentTime(Instant.now());

        final SocketAddress addr = new SocketAddress("localhost", 51115);

        final KeepAliveFrameCounter controlFrameHandler = new KeepAliveFrameCounter();
        serverTransport.createServerSocketBinding(addr)
            .transportChannelHandler(controlFrameHandler)
            .bind();

        final ClientChannelPool channelPool = clientTransport.createClientChannelPool().build();

        final ClientChannel channel = channelPool.requestChannel(addr);
        TestUtil.waitUntil(() -> channel.isOpen());

        ClockUtil.addTime(Duration.ofMillis(KEEP_ALIVE_PERIOD + 1));
        TestUtil.waitUntil(() -> controlFrameHandler.numReceivedKeepAliveFrames == 1);

        // when
        ClockUtil.addTime(Duration.ofMillis(KEEP_ALIVE_PERIOD + 1));

        // then
        TestUtil.waitUntil(() -> controlFrameHandler.numReceivedKeepAliveFrames > 1);
        assertThat(controlFrameHandler.numReceivedKeepAliveFrames).isEqualTo(2);
    }

    protected class KeepAliveFrameCounter implements TransportChannelHandler
    {
        protected int numReceivedKeepAliveFrames = 0;

        @Override
        public void onChannelKeepAlive(TransportChannel channel)
        {
            numReceivedKeepAliveFrames++;
        }

        @Override
        public void onChannelOpened(TransportChannel transportChannel)
        {
        }

        @Override
        public void onChannelClosed(TransportChannel transportChannel)
        {
        }

        @Override
        public void onChannelSendError(TransportChannel transportChannel, DirectBuffer buffer, int offset, int length)
        {
        }

        @Override
        public boolean onChannelReceive(TransportChannel transportChannel, DirectBuffer buffer, int offset, int length)
        {
            return true;
        }

        @Override
        public boolean onControlFrame(TransportChannel transportChannel, DirectBuffer buffer, int offset, int length)
        {
            return true;
        }


    }
}
