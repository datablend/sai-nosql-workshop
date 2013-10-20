package sai.document;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * User: dsuvee
 */
public class MongoDBExample {

    private final Mongo mongoClient;
    private final DB database;
    private final DBCollection userCollection;
    private final String[] genders = new String[] { "male", "female"};
    private final int numberOfUsers = 10000;

    public MongoDBExample() throws UnknownHostException {
        mongoClient = new MongoClient("localhost:27017");
        database = mongoClient.getDB("database");
        userCollection = database.getCollection("users");
        userCollection.createIndex(new BasicDBObject("age",1));
        userCollection.createIndex(new BasicDBObject("name",1));
    }

    // Import random user data
    public void importUserData() {
        long start = System.currentTimeMillis();
        Random r = new Random();
        // Let's generate 10.000 random users and store their data in Redis
        for (int i = 0; i < numberOfUsers; i++) {
            BasicDBObject user = new BasicDBObject();
            user.put("name" , "user" + i);
            user.put("gender", genders[r.nextInt(genders.length)]);
            user.put("age", r.nextInt(90));

            // Lets add a number of random items
            int numberofitems = r.nextInt(100);
            List<DBObject> itemObjects = new ArrayList<DBObject>();
            for (int j = 0; j < numberofitems; j++) {
                DBObject itemObject = new BasicDBObject();
                itemObject.put("name","item" + r.nextInt(1000));
                itemObject.put("amount", r.nextInt(5));
                itemObjects.add(itemObject);
            }
            user.put("items",itemObjects);

            userCollection.insert(user);
        }
        System.out.println("Imported users in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Find users of a certain age
    public void findUsersOfAge(int age) {
        long start = System.currentTimeMillis();
        DBObject query = QueryBuilder.start("age").is(age).get();
        System.out.println(userCollection.find(query).size() + " users found in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Find users of a certain age and gender that bought a certain item
    public void findUsersOfAgeAndItem(int age, int itemId, String gender) {
        long start = System.currentTimeMillis();
        DBObject query = QueryBuilder.start("age").is(age).and("gender").is(gender).and("items.name").is("item" + itemId).get();
        System.out.println(userCollection.find(query).size() + " users found in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Find users of a certain age and gender that did not buy a certain item
    public void findUsersYoungerOfAgeAndNotItem(int age, int itemId, String gender) {
        long start = System.currentTimeMillis();
        DBObject query = QueryBuilder.start("age").lessThan(33).and("gender").is("male").and("items.name").notIn(new String[]{"item500"}).get();
        System.out.println(userCollection.find(query).size() + " users found in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Aggregate the number of items that have been sold
    public void aggregateAllItems() {
        long start = System.currentTimeMillis();

        String map = "function() {  " +
                "var numberofitems = this.items.length; " +
                "for (i = 0; i < numberofitems; i++) { " +
                   "emit (this.items[i].name, this.items[i].amount) "+
                "} " +
                "}";

        String reduce = "function(key, values) { " +
                "return Array.sum(values);" +
                "}";

        MapReduceCommand cmd = new MapReduceCommand(userCollection, map, reduce, null, MapReduceCommand.OutputType.INLINE, null);
        MapReduceOutput out = userCollection.mapReduce(cmd);

        for (DBObject o : out.results()) {
            System.out.println(o.toString());
        }

        System.out.println(" Items aggregate in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Aggregate the number of items that have been sold
    public void aggregateTop100Items() {
        long start = System.currentTimeMillis();

        DBObject[] pipeline = new DBObject[5];

        // Only work on the items, ignore the rest
        pipeline[0] = BasicDBObjectBuilder.start("$project", new BasicDBObject("items",1)).get();

        // Unwinds all items, so that each document contains exactly one item
        pipeline[1] = BasicDBObjectBuilder.start("$unwind","$items").get();

        // Group them on item name and sum the individual amounts
        pipeline[2] = BasicDBObjectBuilder.start("$group", new BasicDBObject("_id", "$items.name").append("total", new BasicDBObject("$sum","$items.amount"))).get();

        // Sort them on total
        pipeline[3] = BasicDBObjectBuilder.start("$sort", new BasicDBObject("total",-1)).get();

        // Only get the top 10
        pipeline[4] = BasicDBObjectBuilder.start("$limit", 10).get();

        DBObject aggregatecommand = BasicDBObjectBuilder.start("aggregate", "users").add("pipeline",pipeline).get();
        CommandResult commandresult = database.command(aggregatecommand);

        System.out.println(commandresult);

        System.out.println(" Items aggregate in " + (System.currentTimeMillis() - start) + " ms");
    }

    public static void main(String[] args) throws UnknownHostException {
        MongoDBExample example = new MongoDBExample();
        // Import user data
        example.importUserData();
        example.findUsersOfAge(33);
        example.findUsersOfAgeAndItem(33, 500, example.genders[0]);
        example.findUsersYoungerOfAgeAndNotItem(33, 500, example.genders[0]);
        example.aggregateAllItems();
        example.aggregateTop100Items();
    }

}
