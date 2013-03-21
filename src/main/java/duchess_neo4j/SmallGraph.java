package duchess_neo4j;


import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.WrappingNeoServerBootstrapper;

import java.io.File;
import java.io.IOException;


public class SmallGraph {

    private static final String DB_PATH = "neo4j_demo/data"; //where you want your data to be stored

    public static final String ACTRESS_PROPERTY = "name";
    public static final String FILM_PROPERTY = "title";

    GraphDatabaseAPI graphdb;

    Index<Node> filmIndex;

    public static void main(String[] args) {

        SmallGraph demo = new SmallGraph();

        demo.clearDB();
        demo.createDB();
        demo.createSomeNodes();
        demo.createMoreWithIndexLookUp();
       // demo.startAsServer();           //use this if you want webserver and webadmin. Do not shutdown db if you use it.
        demo.useCypherToFindActressForFilm("Out of Africa");
        demo.queryWithTraversal();
        demo.shutdownDB();  //   Don't shut down if you run as server :)

    }
    private void createDB() {


        graphdb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphdb);
        System.out.println("Started the database");

        filmIndex = graphdb.index().forNodes("films");



    }


    private void createSomeNodes() {

        String actress = "Meryl Streep";
        String [] films = {"The Iron Lady", "Mamma Mia", "Out of Africa"};

        Transaction tx = graphdb.beginTx();

        try {
            Node actressNode = graphdb.createNode();
            actressNode.setProperty(ACTRESS_PROPERTY,actress);
            System.out.println("Created actress node: "+ actressNode.getId());

            for (String film : films){
                Node filmNode = graphdb.createNode();
                filmNode.setProperty(FILM_PROPERTY,film);

                filmIndex.add(filmNode, FILM_PROPERTY,filmNode.getProperty("title"));

                System.out.println("Created film node: "+ filmNode.getId());

                actressNode.createRelationshipTo(filmNode, RelType.ACTS_IN);
                System.out.println(actressNode.getId()+ "->" + filmNode.getId());
            }



            tx.success();

        } finally {
            tx.finish();
        }


    }

    private void createMoreWithIndexLookUp() {

        String newActress = "Iman";

        Transaction tx = graphdb.beginTx();

        try {
            Node newActressNode = graphdb.createNode();
            newActressNode.setProperty(ACTRESS_PROPERTY, newActress);

            IndexHits<Node> result =  filmIndex.get(FILM_PROPERTY, "Out of Africa");

            if (result.size()!=1){
                System.out.println("Something is wrong! should be just 1");
            } else {

                Node oldFilmNode = result.next();
                newActressNode.createRelationshipTo(oldFilmNode, RelType.ACTS_IN);

            }

            tx.success();
        } finally {
            tx.finish();
        }


    }

    private void queryWithTraversal() {

        TraversalDescription td = Traversal.description()
                .evaluator(Evaluators.excludeStartPosition());

        Traverser result = td.traverse(graphdb.getNodeById(1));
        for (Path p : result){
            System.out.println(p);
        }

        System.out.println();

        TraversalDescription td2 = Traversal.description()
                .depthFirst()
                .relationships(RelType.ACTS_IN, Direction.OUTGOING)
                .evaluator(Evaluators.excludeStartPosition());

        Traverser result2 = td2.traverse(graphdb.getNodeById(1));
        for (Node node : result2.nodes()){
            System.out.println(node.getProperty(FILM_PROPERTY));
        }
    }

    private void useCypherToFindActressForFilm(String title) {

        ExecutionEngine engine = new ExecutionEngine(graphdb);
        ExecutionResult result = engine.execute("START film=node:films(title='" + title + "') MATCH film<-[:ACTS_IN]-actor RETURN actor.name");
        System.out.println(result.dumpToString());

    }

    private void startAsServer() {

        WrappingNeoServerBootstrapper srv = new WrappingNeoServerBootstrapper(graphdb);
        srv.start();


    }




    private void clearDB()
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




    private void shutdownDB() {

        System.out.println("Shutting down the database ...");
        graphdb.shutdown();

    }


}
