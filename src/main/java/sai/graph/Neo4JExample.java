package sai.graph;

import com.google.common.collect.Sets;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.EigenvectorCentralityPower;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;

/**
 * User: dsuvee
 */
public class Neo4JExample {

    private final GraphDatabaseService graph;
    private final IndexManager indexManager;
    private final Index<Node> userIndex;
    private final String[] genders = new String[] { "male", "female"};
    private final int numberOfUsers = 10000;
    private final int numberOfRelations = 100000;
    private enum RelTypes implements RelationshipType {
        is_friend
    }

    public Neo4JExample() {
        graph = new GraphDatabaseFactory().newEmbeddedDatabase("example");
        Transaction t = graph.beginTx();
        indexManager = graph.index();
        userIndex = indexManager.forNodes("users");
        t.success();
        t.close();
    }

    // Import random user data
    public void importUserData() {
        long start = System.currentTimeMillis();
        Transaction t = graph.beginTx();
        try {
            Random r = new Random();
            // Let's generate 10.000 random users and store their data in Neo4J
            for (int i = 0; i < numberOfUsers; i++) {
                Node user = graph.createNode();
                int age = r.nextInt(90);
                user.setProperty("name" , "user" + i);
                user.setProperty("gender", genders[r.nextInt(genders.length)]);
                user.setProperty("age", age);
                userIndex.add(user, "age", age);
            }
            t.success();
        }
        finally {
            t.close();
        }
        System.out.println("Imported users in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Find users of a certain age
    public void findUsersOfAge(int age) {
        Transaction t = graph.beginTx();
        long start = System.currentTimeMillis();
        System.out.println(userIndex.get("age", age).size() + " users found in " + (System.currentTimeMillis() - start) + " ms");
        t.success();
        t.close();
    }

    // Create relationships between random persons
    public void importRelationshipData() {
        long start = System.currentTimeMillis();
        Transaction t = graph.beginTx();
        try {
            Random r = new Random();
            // Let's generate 100000 random relationships
            for (int i = 0; i < numberOfRelations; i++) {
                Node user1 = graph.getNodeById(r.nextInt(10000));
                Node user2 = graph.getNodeById(r.nextInt(10000));
                user1.createRelationshipTo(user2, RelTypes.is_friend);
            }
            t.success();
        }
        finally {
            t.close();
        }
        System.out.println("Created friend relationships in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Find friends of a user
    public void findFriendsOfUser(int userId) {
        Transaction t = graph.beginTx();
        long start = System.currentTimeMillis();
        Iterator<Relationship> it =  graph.getNodeById(userId).getRelationships(Direction.BOTH).iterator();
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        System.out.println(count + " friends found in " + (System.currentTimeMillis() - start) + " ms");
        t.success();
        t.close();
    }

    // Find friends of friends
    public void findFriendsOfFriends(int userId) {
        Transaction t = graph.beginTx();
        long start = System.currentTimeMillis();
        TraversalDescription td =
            Traversal.description().breadthFirst().relationships(RelTypes.is_friend, Direction.BOTH).evaluator(Evaluators.toDepth(2));
        Traverser traverser = td.traverse(graph.getNodeById(userId));
        Set<Node> friendsoffriends = new HashSet<Node>();
        for (Path path : traverser) {
            friendsoffriends.add(path.endNode());
        }
        System.out.println(friendsoffriends.size() + " friends of friends found in " + (System.currentTimeMillis() - start) + " ms");
        t.success();
        t.close();
    }

    // Find friend suggestions
    public void findFriendSugguestions(int userId) {
        Transaction t = graph.beginTx();
        long start = System.currentTimeMillis();
        ExecutionEngine engine = new ExecutionEngine(graph, StringLogger.SYSTEM);
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("id", userId);
        ExecutionResult result = engine.execute("START user = node({id})" +
                                                "MATCH user-[:is_friend*2..2]-friend_of_friend " +
                                                "WHERE NOT (user-[:is_friend]-friend_of_friend) " +
                                                "RETURN friend_of_friend.name, COUNT(*) " +
                                                "ORDER BY COUNT(*) DESC , friend_of_friend.name", parameters);
        ResourceIterator<Map<String, Object>> it = result.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            for (Map.Entry<String, Object> column : row.entrySet()) {
                System.out.println(column.getKey() + ": " + column.getValue());
            }
        }
        System.out.println("Friend suggestions found in " + (System.currentTimeMillis() - start) + " ms");
        t.success();
        t.close();
    }

    // Find the importance of a person in the social network
    public void calculateEigenVectorForUser(int userId) {
        Transaction t = graph.beginTx();
        long start = System.currentTimeMillis();
        EigenvectorCentralityPower eigenvectorcalc = new EigenvectorCentralityPower(
                Direction.BOTH,
                (new CostEvaluator<Double>() {
                    public Double getCost(Relationship arg0, Direction arg1) {
                        return 1.0;
                    }
                }),
                Sets.newHashSet(GlobalGraphOperations.at(graph).getAllNodes()),
                Sets.newHashSet(GlobalGraphOperations.at(graph).getAllRelationships()),
                0.01);
        eigenvectorcalc.setMaxIterations(10000);
        eigenvectorcalc.calculate();
        System.out.println(eigenvectorcalc.getCentrality(graph.getNodeById(userId)));
        System.out.println("Importance of user found in " + (System.currentTimeMillis() - start) + " ms");
        t.success();
        t.close();
    }

    // Shuts down the graph
    public void closeGraph() {
        graph.shutdown();
    }

    public static void main(String[] args) {
        Neo4JExample example = new Neo4JExample();
        // Import user data
        example.importUserData();
        example.findUsersOfAge(33);
        example.importRelationshipData();
        example.findFriendsOfUser(1000);
        example.findFriendsOfFriends(1000);
        example.findFriendSugguestions(1000);
        example.calculateEigenVectorForUser(1000);
        example.closeGraph();
    }

}
