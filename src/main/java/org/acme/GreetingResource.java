package org.acme;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow;

import org.jboss.resteasy.reactive.server.core.LazyResponse.Existing;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.ReactiveQueryResult;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Path("/hello")
public class GreetingResource {
    @Inject
    Cluster cluster;


    @GET
    @Path("/exist")
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> existCheck() {
        var context = Vertx.currentContext();

        // Get a reference to a particular Couchbase bucket and its default collection
        var bucket = cluster.reactive().bucket("travel-sample");
        var collection = bucket.defaultCollection() ;
        
        // Upsert a new document
        var uuid = UUID.randomUUID().toString();
        Mono<MutationResult> mono1 = collection.upsert( uuid, JsonObject.create().put("foo", "bar"));
        Mono<ExistsResult> mono2 = bucket.defaultCollection().exists(uuid);

    
        mono1.subscribe();
        mono2.doOnNext(x -> System.out.println(x.exists()));



        return Uni.createFrom().item("hello");
        // Flow.Publisher mono1AsPublisher = AdaptersToFlow.publisher(mono1);
        // Uni uni1 = Uni.createFrom().publisher(mono1AsPublisher);

        // Flow.Publisher mono2AsPublisher = AdaptersToFlow.publisher(mono2);
        // Uni uni2 = Uni.createFrom().publisher(mono2AsPublisher);



        // return uni1
        //     .flatMap(x -> uni2)
        //     .map(x -> x)
        //     .emitOn(r -> context.runOnContext(r))
        //     .log();
    }


    @GET
    @Path("/reactive")
    @Produces(MediaType.TEXT_PLAIN)
    public Mono<String> reactiveHello() {
        var context = Vertx.currentContext();

        // Get a reference to a particular Couchbase bucket and its default collection
        var bucket = cluster.reactive().bucket("travel-sample");
        var collection = bucket.defaultCollection() ;
        
        // Upsert a new document
        Mono<MutationResult> mono1 = collection.upsert("test", JsonObject.create().put("foo", "bar"));
        Mono<GetResult> mono2 = bucket.defaultCollection().get("test")
            .doOnNext(doc -> System.out.println("Got doc " + doc.contentAs(Map.class).toString()));
        // Perform a N1QL query
        Mono<ReactiveQueryResult> mono3 =  cluster.reactive().query("select * from `travel-sample` where url like 'http://marriot%' and country = 'United States';");

        return mono1
            .flatMap(x -> mono2)
            .flatMap(x -> mono3)
            .doOnNext(x -> {
                x.rowsAsObject().doOnNext(row -> {
                    System.out.println(row);
                });
            })
            .map(x -> "success!!")
            .log();
    }


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> asyncHello() {
        var context = Vertx.currentContext();
        // Get a reference to a particular Couchbase bucket and its default collection
        var bucket = cluster.bucket("travel-sample")
        .async();
        var collection = bucket.defaultCollection() ;

        // Upsert a new document
        var uni1 = Uni.createFrom().completionStage(
            () -> collection.upsert("test", JsonObject.create().put("foo", "bar"))
        );
        var uni2 = Uni.createFrom().completionStage(() -> bucket.defaultCollection().get("test"))
            .invoke(doc -> System.out.println("Got doc " + doc.contentAs(Map.class).toString()));
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
