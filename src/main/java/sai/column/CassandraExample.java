package sai.column;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

/**
 * User: dsuvee
 */
public class CassandraExample {

    private final Cluster cluster;
    private ColumnFamilyTemplate<String, String> userTemplate;
    private ColumnFamilyTemplate<String, String> followerTemplate;
    private ColumnFamilyTemplate<String, UUID> tweetTemplate;
    private ColumnFamilyTemplate<String, UUID> timelineTemplate;

    private final String TWITTER_KEYSPACE = "twitter";
    private final String USERS_COLUMNFAMILY = "users";
    private final String TWEETS_COLUMNFAMILY = "tweets";
    private final String FOLLOWERS_COLUMNFAMILY = "followers";
    private final String TIMELINE_COLUMNFAMILY = "timeline";
    private final String[] genders = new String[] { "male", "female"};
    private final int numberOfUsers = 10000;
    private final int numberOfTweets = 10000;

    public CassandraExample() {
        cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        setupKeySpaces();
    }

    public void setupKeySpaces() {
        // Drop it if it already exists
        if (cluster.describeKeyspace(TWITTER_KEYSPACE) != null) {
            cluster.dropKeyspace(TWITTER_KEYSPACE);
        }

        ColumnFamilyDefinition userColumnFamily = HFactory.createColumnFamilyDefinition(TWITTER_KEYSPACE, USERS_COLUMNFAMILY, ComparatorType.ASCIITYPE);
        ColumnFamilyDefinition followerColumnFamily = HFactory.createColumnFamilyDefinition(TWITTER_KEYSPACE, FOLLOWERS_COLUMNFAMILY, ComparatorType.ASCIITYPE);
        ColumnFamilyDefinition tweetsColumnFamily = HFactory.createColumnFamilyDefinition(TWITTER_KEYSPACE, TWEETS_COLUMNFAMILY, ComparatorType.TIMEUUIDTYPE);
        ColumnFamilyDefinition timelineColumnFamily = HFactory.createColumnFamilyDefinition(TWITTER_KEYSPACE, TIMELINE_COLUMNFAMILY, ComparatorType.TIMEUUIDTYPE);
        KeyspaceDefinition twitterKeySpace = HFactory.createKeyspaceDefinition(TWITTER_KEYSPACE,ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(userColumnFamily, followerColumnFamily, tweetsColumnFamily, timelineColumnFamily));
        cluster.addKeyspace(twitterKeySpace, true);

        Keyspace keyspace = HFactory.createKeyspace(TWITTER_KEYSPACE, cluster);

        userTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace, USERS_COLUMNFAMILY, StringSerializer.get(), StringSerializer.get());
        followerTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace, FOLLOWERS_COLUMNFAMILY, StringSerializer.get(), StringSerializer.get());
        tweetTemplate = new ThriftColumnFamilyTemplate<String, UUID>(keyspace, TWEETS_COLUMNFAMILY, StringSerializer.get(), TimeUUIDSerializer.get());
        timelineTemplate = new ThriftColumnFamilyTemplate<String, UUID>(keyspace, TIMELINE_COLUMNFAMILY, StringSerializer.get(), TimeUUIDSerializer.get());
    }

    // Import random user data
    public void importUserData() {
        long start = System.currentTimeMillis();
        Random r = new Random();
        // Let's generate 10.000 random users and store their data in cassandra
        for (int i = 0; i < numberOfUsers; i++) {
            String userKey = "user" + ":" + i;
            ColumnFamilyUpdater<String, String> updater = userTemplate.createUpdater(userKey);
            updater.setString("name", "user" + i);
            updater.setString("gender", genders[r.nextInt(genders.length)]);
            updater.setInteger("age", r.nextInt(90));
            userTemplate.update(updater);
        }
        System.out.println("Imported users in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Create random followers
    public void importFollowerData() {
        long start = System.currentTimeMillis();
        Random r = new Random();
        userTemplate.setBatched(true);
        // Let's generate 10.000 random users and store their data in cassandra
        for (int i = 0; i < numberOfUsers; i++) {
            String userKey = "user" + ":" + i;
            ColumnFamilyUpdater<String, String> updater = followerTemplate.createUpdater(userKey);
            // Give each users a random number of followers between 0 and 10
            int numberOfFollowers = r.nextInt(10);
            for (int j = 0; j < numberOfFollowers; j++) {
                updater.setString("user" + ":" + r.nextInt(numberOfUsers), "");
            }
            followerTemplate.update(updater);
        }
        System.out.println("Imported followers in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Create random tweets
    public void importTweetData() {
        long start = System.currentTimeMillis();
        Random r = new Random();
        // Let's generate 10.000 random tweets
        for (int i = 0; i < numberOfTweets; i++) {
            // For a random user
            String userKey = "user" + ":" + r.nextInt(10000);
            // Create a random tweet
            ColumnFamilyUpdater<String, UUID> updater = tweetTemplate.createUpdater(userKey);
            UUID timeUuid = new UUID();
            updater.setString(timeUuid, timeUuid.toString());
            tweetTemplate.update(updater);

            // Get all followers of a certain user and add the tweet to their timeline
            Collection<String> followers = followerTemplate.queryColumns(userKey).getColumnNames();
            for (String follower : followers) {
                ColumnFamilyUpdater<String, UUID> timelineTemplateUpdater = timelineTemplate.createUpdater(follower);
                timelineTemplateUpdater.setString(timeUuid, timeUuid.toString());
                timelineTemplate.update(timelineTemplateUpdater);
            }
        }
        System.out.println("Imported tweets in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Get tweets for a particular user
    public void getTweetsForUser(int userId) {
        long start = System.currentTimeMillis();
        String userKey = "user" + ":" + userId;
        ColumnFamilyResult<String, UUID> tweets = tweetTemplate.queryColumns(userKey);
        for (UUID uuid : tweets.getColumnNames()) {
            System.out.println(uuid.toString());
        }
        System.out.println("Retrieved tweets in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Get timeline for a particular user
    public void getTimelineForUser(int userId) {
        long start = System.currentTimeMillis();
        String userKey = "user" + ":" + userId;
        ColumnFamilyResult<String, UUID> tweets = timelineTemplate.queryColumns(userKey);
        for (UUID uuid : tweets.getColumnNames()) {
            System.out.println(uuid.toString());
        }
        System.out.println("Retrieved timeline in " + (System.currentTimeMillis() - start) + " ms");
    }

    public static void main(String[] args) {
        CassandraExample cassandraExample = new CassandraExample();
        // Import user data
        cassandraExample.importUserData();
        cassandraExample.importFollowerData();
        cassandraExample.importTweetData();
        cassandraExample.getTweetsForUser(1000);
        cassandraExample.getTimelineForUser(1000);
    }

}
