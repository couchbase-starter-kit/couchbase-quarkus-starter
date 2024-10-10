package org.acme;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/hello")
public class GreetingResource {
    @Inject
    Cluster cluster;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> hello() {
        var context = Vertx.currentContext();
        // Get a reference to a particular Couchbase bucket and its default collection
        var bucket = cluster.bucket("travel-sample")
        .async();
        var collection = bucket.defaultCollection() ;

        // Upsert a new document
        var uni1 = Uni.createFrom().completionStage(() -> collection.upsert("test", JsonObject.create().put("foo", "bar")));
        var uni2 = Uni.createFrom().completionStage(() -> bucket.defaultCollection().get("test"))
            .invoke(doc -> System.out.println("Got doc " + doc.contentAsObject().toString()));
        // Perform a N1QL query
        var uni3 = Uni.createFrom().completionStage(() -> cluster.async().query("select * from `travel-sample` where url like 'http://marriot%' and country = 'United States';"));

        return uni1
            .flatMap(x -> uni2)
            .flatMap(x -> uni3)
            .invoke(x -> {
                x.rowsAsObject().forEach(row -> {
                    System.out.println(row);
                });
            })
            .map(x -> "success!!")
            .emitOn(r -> context.runOnContext(r))
            .log();
            }
}
