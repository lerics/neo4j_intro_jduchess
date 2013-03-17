package duchess_neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.WrappingNeoServerBootstrapper;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: luer01
 * Date: 3/17/13
 * Time: 7:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class SmallGraph {

    private static final String  DB_PATH = "duchess_neo4j";
    EmbeddedGraphDatabase graphdb;

    Index <Node> filmIndex;

    public static void main(String[] args) {

        SmallGraph demo = new SmallGraph();

        demo.clearDb();
        demo.createDb();
        demo.createSomeNodes();
        demo.createMoreNodes();

        //demo.queryWithCypher("Out of Africa");
        demo.queryWithTraversal();

      //  demo.runAsServer();

        demo.shutdown();

    }

    private void queryWithTraversal() {

        TraversalDescription td = Traversal.description()
                .depthFirst()
                .evaluator(Evaluators.excludeStartPosition())
                .relationships(RelType.ACTS_IN, Direction.OUTGOING);

        Traverser result = td.traverse(graphdb.getNodeById(1));

        for (Path path : result){
            System.out.println(path + "\n");
        }

        for (Node node : result.nodes()){
            System.out.println(node.getProperty("title"));
        }



    }

    private void queryWithCypher(String title) {

        ExecutionEngine engine = new ExecutionEngine(graphdb);
        ExecutionResult result = engine.execute("START me=node:films(title='"+title+"') MATCH me<-[:ACTS_IN]-actor RETURN actor.name");
        System.out.println(result.dumpToString());

    }



    private void createMoreNodes() {

        String name = "Iman";

        Transaction tx = graphdb.beginTx();

        try {
            Node newActorNode = graphdb.createNode();
            newActorNode.setProperty("name", name);

            IndexHits<Node> result = filmIndex.get("title", "Out of Africa");

            if (result.size() <1 || result.size()>1){

                System.out.println("Smth is wrong. there should be one and only result here");

            }   else {

                for (Node oldFilmNode : result){
                    newActorNode.createRelationshipTo(oldFilmNode,RelType.ACTS_IN);
                }

            }

            tx.success();
        } finally {
            tx.finish();
        }


    }

    private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively(new File(DB_PATH));
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void runAsServer() {

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(graphdb);
        srv.start();
    }


    private void createSomeNodes() {

    String [] films = {"The Iron Lady", "Mamma Mia", "Out of Africa"};

     Transaction tx = graphdb.beginTx();
     Node actorNode;

        try {

                actorNode = graphdb.createNode();
                actorNode.setProperty("name", "Meryl Streep");

                System.out.println("Created an actor node: " + actorNode.toString());



            for (String film : films){

                Node filmNode = graphdb.createNode();
                filmNode.setProperty("title", film);
                filmIndex.add(filmNode,"title",film);

                System.out.println("Created a film node: " + filmNode.toString());

                actorNode.createRelationshipTo(filmNode,RelType.ACTS_IN);


            }

            tx.success();

        } finally {

            tx.finish();

        }


    }

    private void shutdown() {

        graphdb.shutdown();
    }

    private void createDb() {

        graphdb = new EmbeddedGraphDatabase(DB_PATH);

        filmIndex = graphdb.index().forNodes( "films" );

        registerShutdownHook(graphdb);

    }

    private void registerShutdownHook(final GraphDatabaseService graphdb) {

        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphdb.shutdown();
            }
        });

    }

}
