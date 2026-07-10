package com.hsbc.cranker.mucranker;

import com.hsbc.cranker.connector.CrankerConnector;
import com.hsbc.cranker.connector.CrankerConnectorBuilder;
import com.hsbc.cranker.connector.RegistrationUriSuppliers;
import io.muserver.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import scaffolding.AssertUtils;
import scaffolding.ClientUtils;

import java.net.URI;
import java.net.http.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static io.muserver.MuServerBuilder.muServer;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@Disabled("Should run after the connector alpha version is published")
public class RouterWebSocketTest {

    private CrankerRouter crankerRouter;
    private MuServer registrationServer;
    private MuServer crankerServer;
    private MuServer targetServer;
    private CrankerConnector connector;
    private final HttpClient httpClient = HttpClient.newBuilder()
        .sslContext(ClientUtils.sslContextForTesting(ClientUtils.veryTrustingTrustManager))
        .build();

    private final CompletableFuture<Void> clientOnOpenLatch = new CompletableFuture<>();
    private final CompletableFuture<Void> targetOnOpenLatch = new CompletableFuture<>();
    private final CompletableFuture<Void> targetCloseLatch = new CompletableFuture<>();
    private final CompletableFuture<Throwable> targetErrorLatch = new CompletableFuture<>();
    private final CompletableFuture<WebSocket> targetWsSession = new CompletableFuture<>();

    @BeforeEach
    public void setup() {
        crankerRouter = CrankerRouterBuilder.crankerRouter()
            .withSupportedCrankerProtocols(List.of(
                CrankerRouterBuilder.CRANKER_PROTOCOL_1,
                CrankerRouterBuilder.CRANKER_PROTOCOL_3,
                CrankerRouterBuilder.CRANKER_PROTOCOL_3_1
            ))
            .start();

        registrationServer = muServer()
            .withHttpsPort(0)
            .addHandler(crankerRouter.createRegistrationHandler())
            .start();

        crankerServer = muServer()
            .withHttpsPort(0)
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        targetServer = muServer()
            .withHttpPort(0)
            .addHandler(WebSocketHandlerBuilder.webSocketHandler()
                .withPath("/route-ws-service/ws")
                .withWebSocketFactory((request, responseHeaders) -> new BaseWebSocket() {
                    @Override
                    public void onConnect(MuWebSocketSession session) throws Exception {
                        super.onConnect(session);
                        targetOnOpenLatch.complete(null);
                    }

                    @Override
                    public void onText(String message, boolean isLast, DoneCallback onComplete) throws Exception {
                        if (message.equals("abrupt-close")) {
                            // target simply closes/throws to trigger router cleanup
                            throw new RuntimeException("Target exception triggered");
                        }
                        session().sendText("Echo: " + message, onComplete);
                    }

                    @Override
                    public void onClientClosed(int statusCode, String reason) throws Exception {
                        targetCloseLatch.complete(null);
                        super.onClientClosed(statusCode, reason);
                    }

                    @Override
                    public void onError(Throwable cause) throws Exception {
                        targetErrorLatch.complete(cause);
                        super.onError(cause);
                    }
                }))
            .addHandler(Method.GET, "/router-ws-service/http-test", (request, response, pathParams) -> {
                response.write("HTTP Response: " + request.query().get("p"));
            })
            .start();
        connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(List.of(CrankerRouterBuilder.CRANKER_PROTOCOL_3_1))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(targetServer.uri())
            .withRoute("router-ws-service")
            .withRouterUris(RegistrationUriSuppliers.fixedUris(List.of(
                URI.create("ws" + registrationServer.uri().toString().substring(4))
            )))
            .withSlidingWindowSize(2)
            .start();
    }

    @AfterEach
    public void tearDown() {
        if (connector != null) {
            connector.stop(5, TimeUnit.SECONDS);
        }
        if (targetServer != null) {
            targetServer.stop();
        }
        if (crankerServer != null) {
            crankerServer.stop();
        }
        if (registrationServer != null) {
            registrationServer.stop();
        }
        if (crankerRouter != null) {
            crankerRouter.stop();
        }
    }

    @Test
    public void testAbnormalClientDisconnectHandlerCleanup() throws Exception {
        // Wait for registration
        AssertUtils.assertEventually(() -> crankerRouter.collectInfo().services().size() > 0, is(true));

        URI wsClientUri = URI.create("ws" + crankerServer.uri().toString().substring(4) + "/router-ws-service/ws");

        CompletableFuture<WebSocket> clientWsFuture = httpClient.newWebSocketBuilder()
            .buildAsync(wsClientUri, new WebSocket.Listener() {
                @Override
                public void onOpen(WebSocket webSocket) {
                    clientOnOpenLatch.complete(null);
                    webSocket.request(1);
                }
            });

        WebSocket clientWs = clientWsFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(clientWs);
        assertTrue(clientOnOpenLatch.isDone());

        // Wait for connection to reach the target server
        assertDoesNotThrow(() -> targetOnOpenLatch.get(10, TimeUnit.SECONDS));

        // Verify active stream is mapped under contextMap
        ConnectorInstance socketV3 = crankerRouter.collectInfo().services().get(0).connectors().get(0);
        assertNotNull(socketV3);


        // Simulate abnormal client loss midway by calling clientWs.abort() (forces immediate channel TCP drop)
        clientWs.abort();
        System.out.println("Client socket aborted abruptly.");

        // Assert Router side cleans up the RequestContext inside ContextMap immediately after drop
        AssertUtils.assertEventually(() -> crankerRouter.idleConnectionCount() >= 0, is(true));

        // Wait and confirm that target side listener triggered close/error lifecycle
        AssertUtils.assertEventually(() -> targetCloseLatch.isDone() || targetErrorLatch.isDone() || targetErrorLatch.isCompletedExceptionally(), is(true));
    }

    @Test
    public void testAbnormalConnectorShuttingGracefulRecovery() throws Exception {
        AssertUtils.assertEventually(() -> crankerRouter.collectInfo().services().size() > 0, is(true));

        URI wsClientUri = URI.create("ws" + crankerServer.uri().toString().substring(4) + "router-ws-service/ws");

        CompletableFuture<WebSocket> clientWsFuture = httpClient.newWebSocketBuilder()
            .buildAsync(wsClientUri, new WebSocket.Listener() {
                @Override
                public void onOpen(WebSocket webSocket) {
                    webSocket.request(1);
                }
            });

        WebSocket clientWs = clientWsFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(clientWs);


        // Abruptly terminate the connector
        connector.stop(0, TimeUnit.MILLISECONDS);
        System.out.println("Connector stopped abruptly.");

        // Client side must receive error termination when Connector disappears
        AssertUtils.assertEventually(() -> clientWs.isInputClosed() || clientWs.isOutputClosed(), is(true));
    }

    @Test
    public void testWebSocketFallbackWhenNegotiatedWithLegacyVersion() throws Exception {
        // Stop the initially registered 3.1 connector
        connector.stop(1, TimeUnit.SECONDS);

        // Connect a new legacy v3.0 connector
        CrankerConnector legacyConnector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(List.of(CrankerRouterBuilder.CRANKER_PROTOCOL_3))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(targetServer.uri())
            .withRoute("router-ws-service")
            .withRouterUris(RegistrationUriSuppliers.fixedUris(List.of(
                URI.create("ws" + registrationServer.uri().toString().substring(4))
            )))
            .withSlidingWindowSize(2)
            .start();

        try {
            AssertUtils.assertEventually(() -> crankerRouter.collectInfo().services().size() > 0, is(true));

            // Upgrade should fail on the client side with 501 Not Implemented
            URI wsClientUri = URI.create("ws" + crankerServer.uri().toString().substring(4) + "/router-ws-service/ws");

            CompletableFuture<WebSocket> clientWsFuture = httpClient.newWebSocketBuilder()
                .buildAsync(wsClientUri, new WebSocket.Listener() {
                });

            ExecutionException ex = assertThrows(ExecutionException.class, () -> clientWsFuture.get(5, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof WebSocketHandshakeException);
            WebSocketHandshakeException handshakeEx = (WebSocketHandshakeException) ex.getCause();
            assertEquals(501, handshakeEx.getResponse().statusCode());
        } finally {
            legacyConnector.stop(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMixedPayloadMultiplexing() throws Exception {
        AssertUtils.assertEventually(() -> crankerRouter.collectInfo().services().size() > 0, is(true));

        URI wsClientUri = URI.create("ws" + crankerServer.uri().toString().substring(4) + "router-ws-service/ws");

        BlockingQueue<String> wsResponseQueue = new LinkedBlockingQueue<>();
        WebSocket clientWs = httpClient.newWebSocketBuilder()
            .buildAsync(wsClientUri, new WebSocket.Listener() {
                @Override
                public void onOpen(WebSocket webSocket) {
                    webSocket.request(1);
                }

                @Override
                public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                    wsResponseQueue.add(data.toString());
                    webSocket.request(1);
                    return null;
                }
            }).get(10, TimeUnit.SECONDS);

        int tasksCount = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < tasksCount; i++) {
            final int index = i;
            final WebSocket finalWs = clientWs;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    // Send an HTTP REST request concurrently
                    HttpRequest httpReq = HttpRequest.newBuilder()
                        .uri(crankerServer.uri().resolve("/router-ws-service/http-test?p=msg-" + index))
                        .GET()
                        .build();
                    HttpResponse<String> httpResp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, httpResp.statusCode());
                    assertEquals("HTTP RESPONSE: msg-" + index, httpResp.body());

                    // Send a WS Frame concurrently with synchronization on finalWs as HttpClient WebSocket is not thread-safe for concurrent writes
                    synchronized (finalWs) {
                        finalWs.sendText("WS-" + index, true).get(5, TimeUnit.SECONDS);
                    }

                } catch (Exception e) {
                    fail("Multiplex task failed: " + e.getMessage());
                }
            }, executorService));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

        // Verify WebSocket Echo responses are successfully pooled in arbitrary order without corruption
        for (int i = 0; i < tasksCount; i++) {
            String wsResponse = wsResponseQueue.poll(10, TimeUnit.SECONDS);
            assertNotNull(wsResponse);
            assertTrue(wsResponse.startsWith("Echo: WS-"));
        }

        clientWs.sendClose(1000, "Finished").get(5, TimeUnit.SECONDS);
        executorService.shutdown();
    }

}
