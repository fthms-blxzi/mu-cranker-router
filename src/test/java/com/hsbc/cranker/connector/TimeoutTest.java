/**
 * Move the test under com.hsbc.cranker.jdkconnector package so that we can access the connector class,
 * for testing the abrupt disconnect scenarios.
 *
 * @see com.hsbc.cranker.jdkconnector.TimeoutTest#ifTheConnectorDisconnectsWithoutGracefulShutdownItIsEventuallyDetected()
 */
package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.BaseEndToEndTest;
import com.hsbc.cranker.mucranker.CrankerRouter;
import com.hsbc.cranker.mucranker.ProxyInfo;
import com.hsbc.cranker.mucranker.ProxyListener;
import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import io.muserver.ResponseState;
import okhttp3.Response;
import okhttp3.internal.http2.StreamResetException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.condition.DisabledIf;
import scaffolding.RustTestHelper;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hsbc.cranker.connector.ConnectorSocket.State.IDLE;
import static com.hsbc.cranker.mucranker.BaseEndToEndTest.preferredProtocols;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;
import static scaffolding.ClientUtils.call;
import static scaffolding.ClientUtils.request;
import static scaffolding.TestServerBuilder.crankerRouter;
import static scaffolding.TestServerBuilder.httpServer;
import static scaffolding.TestServerBuilder.httpsServer;

public class TimeoutTest {

    static {
        System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");
    }

    private CrankerRouter router;
    private MuServer routerServer;
    private CrankerConnector connector;
    private MuServer target;

    @AfterEach
    public void cleanup() {
        if (connector != null) swallowException(() -> connector.stop(5, TimeUnit.SECONDS));
        if (target != null) swallowException(target::stop);
        if (routerServer != null) swallowException(routerServer::stop);
        if (router != null) swallowException(router::stop);
    }

    @RepeatedTest(3)
    public void ifTheIdleTimeoutIsExceededBeforeResponseStartedThenA504IsReturned(RepetitionInfo repetitionInfo) throws IOException {
        router = crankerRouter()
            .withIdleTimeout(250, TimeUnit.MILLISECONDS)
            .withSupportedCrankerProtocols(List.of("cranker_1.0", "cranker_3.0"))
            .start();
        routerServer = httpsServer()
            .addHandler(router.createRegistrationHandler())
            .addHandler(router.createHttpHandler())
            .start();

        target = httpServer()
            .addHandler(Method.GET, "/my-app/sleep-without-response",
                (request, response, pathParams) -> Thread.sleep(500))
            .start();
        connector = startConnector("my-app",  preferredProtocols(repetitionInfo));
        try (Response resp = call(request(routerServer.uri().resolve("/my-app/sleep-without-response")))) {
            assertThat(resp.code(), is(504));
        }
    }

    @RepeatedTest(6)
    public void ifTheIdleTimeoutIsExceededAfterResponseStartedThenConnectionIsClosed(RepetitionInfo repetitionInfo) {
        // FIXME: This test requires the router http server is http2 disabled
        //  otherwise the IOException thrown will not be an EOFException, but instead a StreamResetException
        //  By default mu-server doesn't enable http2
        //  This is an okhttp behaviour, may subject to change in the future okhttp version
        var doHttp2 = doHttp2OrNot(repetitionInfo);
        router = crankerRouter()
            .withIdleTimeout(1450, TimeUnit.MILLISECONDS)
            .withSupportedCrankerProtocols(List.of("cranker_1.0", "cranker_3.0"))
            .withHttp2(doHttp2)
            .start();
        routerServer = httpsServer()
            .addHandler(router.createRegistrationHandler())
            .addHandler(router.createHttpHandler())
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(doHttp2))
            .start();

        target = httpServer()
            .addHandler(Method.GET, "/my-app/send-chunk-then-sleep",
                (request, response, pathParams) -> {
                    response.sendChunk("hi");
                    Thread.sleep(1700);
                    response.sendChunk("bye");
                })
            .start();
        connector = startConnector("my-app",  preferredProtocols(repetitionInfo));
        try (Response resp = call(request(routerServer.uri().resolve("/my-app/send-chunk-then-sleep")))) {
            assertThat(resp.code(), is(200));
            assert resp.body() != null;
            resp.body().string();
            fail("should throw exception already.");
        } catch (IOException expected) {
            if (!doHttp2) {
                assertInstanceOf(EOFException.class, expected);
            } else {
                assertInstanceOf(StreamResetException.class, expected);
            }
        }
    }

    private static boolean doHttp2OrNot(RepetitionInfo repetitionInfo) {
        return repetitionInfo.getCurrentRepetition() % 2 == 0;
    }

    @RepeatedTest(3)
    public void ifTheConnectorDisconnectsWithoutGracefulShutdownItIsEventuallyDetected(RepetitionInfo repetitionInfo) throws Exception {
        router = crankerRouter()
            .withIdleTimeout(1450, TimeUnit.MILLISECONDS)
            .withSupportedCrankerProtocols(List.of("cranker_1.0", "cranker_3.0"))
            .start();
        routerServer = httpsServer()
            .addHandler(router.createRegistrationHandler())
            .addHandler(router.createHttpHandler())
            .start();

        target = httpServer()
            .addHandler(Method.GET, "/my-app/send-chunk-then-sleep",
                (request, response, pathParams) -> {
                    response.sendChunk("hi");
                    Thread.sleep(1700);
                    response.sendChunk("bye");
                })
            .start();

        final List<String> preferredProtocols = preferredProtocols(repetitionInfo);
        connector = startConnector("my-app-x", preferredProtocols);

        assertThat(router.collectInfo().service("my-app-x").get().connectors(), hasSize(1));
        final Collection<ConnectorSocket> idleSockets = connector.routers().get(0).idleSockets();
        assertThat(idleSockets.size(), is(2));

        // Quite hacky! Stop the connector's webclient before the connector can do a graceful shutdown

        for (ConnectorSocket connectorSocket : idleSockets) {
            ConnectorSocketAdapter socket = (ConnectorSocketAdapter) connectorSocket;
            assertEventually(socket::state, is(IDLE));

            switch (preferredProtocols.get(0)) {
                case "cranker_3.0": {
                    final Field connectorSocketField = ConnectorSocketAdapter.class.getDeclaredField("underlying");
                    connectorSocketField.setAccessible(true);
                    final Field webSocketFieldOfV3 = ConnectorSocketV3.class.getDeclaredField("webSocket");
                    webSocketFieldOfV3.setAccessible(true);
                    final ConnectorSocketV3 connectorSocketImpl = (ConnectorSocketV3)connectorSocketField.get(socket);
                    final WebSocket websocket = (WebSocket) webSocketFieldOfV3.get(connectorSocketImpl);
                    websocket.abort();
                    break;
                }
                case "cranker_1.0": {
                    final Field connectorSocketField = ConnectorSocketAdapter.class.getDeclaredField("underlying");
                    connectorSocketField.setAccessible(true);
                    final Field webSocketFieldOfV1 = ConnectorSocketImpl.class.getDeclaredField("webSocket");
                    webSocketFieldOfV1.setAccessible(true);
                    final ConnectorSocketImpl connectorSocketImpl = (ConnectorSocketImpl)connectorSocketField.get(socket);
                    final WebSocket websocket = (WebSocket) webSocketFieldOfV1.get(connectorSocketImpl);
                    websocket.abort();
                    break;
                }
                default: {
                    throw new IllegalStateException("protocol not supported!");
                }
            }
        }

        // without stopping the connector
        assertEventually(() -> router.collectInfo().service("my-app-x").get().connectors(), empty());
    }

    @RepeatedTest(3)
    @DisabledIf("scaffolding.RustTestHelper#isRustMode")
    public void ifClientDisconnectedBeforeResponseStartThenProxyListenersShouldInvoke(RepetitionInfo repetitionInfo) throws InterruptedException {
        AtomicReference<ProxyInfo> ref = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        router = crankerRouter()
            .withProxyListeners(Collections.singletonList(new ProxyListener() {
                @Override
                public void onComplete(ProxyInfo proxyInfo) {
                    ref.set(proxyInfo);
                    countDownLatch.countDown();
                }
            }))
            .withSupportedCrankerProtocols(List.of("cranker_1.0", "cranker_3.0"))
            .start();
        routerServer = httpsServer()
            .addHandler(router.createRegistrationHandler())
            .addHandler(router.createHttpHandler())
            .start();

        String url = "/my-app/sleep-without-response";
        target = httpServer()
            .addHandler(Method.GET, url,
                (request, response, pathParams) -> {
                    Thread.sleep(500);
                    response.write("ok!");
                })
            .start();
        connector = startConnector("my-app",  preferredProtocols(repetitionInfo));

        long callerTimeout = 400L;
        try (Response ignored = call(request(routerServer.uri().resolve(url)), Duration.ofMillis(callerTimeout))) {
            fail("Call should have timed out");
        } catch (Exception e) {
            // expected
        } finally {
            assertThat(countDownLatch.await(1000, TimeUnit.MILLISECONDS), is(true));
            assertThat(ref.get(), notNullValue());
            assertThat(ref.get().response().responseState(), is(ResponseState.CLIENT_DISCONNECTED));
        }
    }

    private CrankerConnector startConnector(String targetServiceName, List<String> preferredProtocols) {
        return BaseEndToEndTest.startConnectorAndWaitForRegistration(router, "*", target, preferredProtocols, targetServiceName, routerServer);
    }

}

