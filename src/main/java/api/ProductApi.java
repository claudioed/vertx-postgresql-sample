package api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * @author Claudio E. de Oliveira on 24/08/15.
 */
public class ProductApi extends AbstractVerticle{

    private final Integer port;

    public ProductApi(Integer port) {
        this.port = port;
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx(new VertxOptions());
        vertx.deployVerticle(new ProductApi(9004));
    }

    @Override
    public void start() throws Exception {
        
        final JDBCClient jdbc = JDBCClient.createNonShared(vertx, new JsonObject()
                .put("url", "jdbc:postgresql://localhost:5432/products")
                .put("user", "postgres")
                .put("password", "postgres")
                .put("driver_class", "org.postgresql.Driver"));

        final Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());

        //Handling connection on products endpoints
        router.route("/api/product*").handler(ctx -> jdbc.getConnection(res -> {
            if (res.failed()) {
                ctx.fail(res.cause());
            } else {
                SQLConnection conn = res.result();
                ctx.put("conn", conn);
                ctx.addHeadersEndHandler(done -> conn.close(close -> {
                    if (close.failed()) {
                        done.fail(close.cause());
                    } else {
                        done.complete();
                    }
                }));
                ctx.next();
            }
        })).failureHandler(routingContext -> {
            SQLConnection conn = routingContext.get("conn");
            if (conn != null) {
                conn.close(v -> {
                });
            }
        });

        router.post("/api/product").handler(ctx -> {
            HttpServerResponse response = ctx.response();
            SQLConnection conn = ctx.get("conn");
            conn.updateWithParams("INSERT INTO products (product) VALUES (?::JSON)",
                    new JsonArray().add(ctx.getBodyAsString()), query-> {
                        if (query.failed()) {
                            ctx.fail(query.cause());
                            return;
                        }
                        response.setStatusCode(201).end();
                    });
        });

        vertx.createHttpServer().requestHandler(router::accept).listen(port);

    }
}
