package com.hsbc.cranker.mucranker;

import com.hsbc.cranker.connector.CrankerConnector;
import io.muserver.MuServer;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import java.util.List;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static scaffolding.TestServerBuilder.httpServer;
import static scaffolding.TestServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static scaffolding.ClientUtils.call;
import static scaffolding.ClientUtils.request;

public class RustCrankerRouterPoCTest {

    @Test
    public void simpleProxyTest() throws Exception {
        System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");

        // 1. Start target server
        MuServer targetServer = httpServer()
            .addHandler(io.muserver.Method.GET, "/hello", (request, response, pathParams) -> {
                response.write("world");
            })
            .start();

        // 2. Start cranker router (Rust under the hood if system property is set)
        CrankerRouter crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("1.0", "3.0"))
            .start();

        // 3. Start registration and visit servers
        MuServer registrationServer = httpsServer()
            .addHandler(crankerRouter.createRegistrationHandler())
            .start();
        MuServer router = httpServer()
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        // 4. Start connector
        CrankerConnector connector = BaseEndToEndTest.startConnectorAndWaitForRegistration(
            crankerRouter, "*", targetServer, List.of("cranker_1.0"), "*", registrationServer
        );

        // 5. Test request
        try (Response resp = call(request(router.uri().resolve("/hello")))) {
            assertThat(resp.code(), is(200));
            assertThat(resp.body().string(), is("world"));
        } finally {
            try { connector.stop(5, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception ignored) {}
            try { targetServer.stop(); } catch (Exception ignored) {}
            try { crankerRouter.stop(); } catch (Exception ignored) {}
            try { registrationServer.stop(); } catch (Exception ignored) {}
            try { router.stop(); } catch (Exception ignored) {}
        }
    }
}
