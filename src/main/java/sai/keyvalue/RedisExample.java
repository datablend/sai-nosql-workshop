package sai.keyvalue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * User: dsuvee
 */
public class RedisExample {

    private final Jedis jedis;
    private final String[] genders = new String[] { "male", "female"};
    private final int numberOfUsers = 10000;

    public RedisExample() {
        jedis = new Jedis("localhost");
    }

    // Import random user data
    public void importUserData() {
        long start = System.currentTimeMillis();
        Pipeline p = jedis.pipelined();
        Random r = new Random();
        // Let's generate 10.000 random users and store their data in Redis
        for (int i = 0; i < numberOfUsers; i++) {
            String userKey = "user" + ":" + i;
            p.hset(userKey, "name" , "user" + i);
            p.hset(userKey, "gender", genders[r.nextInt(genders.length)]);
            p.hset(userKey, "age", r.nextInt(90) + "");
        }
        p.sync();
        System.out.println("Imported users in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Import random user scores
    public void importUserScores() {
        long start = System.currentTimeMillis();
        Pipeline p = jedis.pipelined();
        Random r = new Random();
        // Let's generate 1 million user scores and store them in redis
        for (int i = 0; i < 1000000; i++) {
            // Generate a random user and store the new score in it's list of scores
            int userId = r.nextInt(numberOfUsers);
            // Random score between 0 and 100000000
            int score = r.nextInt(100000000);
            String userScoresKey = "user" + ":" + userId + ":" + "scores";
            p.lpush(userScoresKey, score + "");
            // Make sure only the 100 recent scores are saved
            p.ltrim(userScoresKey, 0, 99);
        }
        p.sync();
        System.out.println("Imported user scores in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Update the high score of each user
    public void updateUserHighScores() {
        long start = System.currentTimeMillis();
        // Find the high scores for each user
        for (int i = 0; i < numberOfUsers; i++) {
            String userScoresKey = "user" + ":" + i + ":" + "scores";
            // Find the highscore of a particular user
            long numberOfScores = jedis.llen(userScoresKey);
            List<String> scores = jedis.lrange(userScoresKey, 0, numberOfScores);
            int highscore = 0;
            for (String score : scores) {
                Integer currentScore = Integer.parseInt(score);
                if (currentScore > highscore) {
                    highscore = currentScore;
                }
            }
            // Update the highscores of the user, but only store it for a day
            String userHighScoreKey = "user" + ":" + i + ":" + "highscore";
            jedis.set(userHighScoreKey, highscore + "");
            jedis.expire(userHighScoreKey, 24 * 60 * 60);
        }
        System.out.println("Updated user high scores in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Update the total high scores overview
    public void updateHighScores() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfUsers; i++) {
            String userHighScoreKey = "user" + ":" + i + ":" + "highscore";
            String userHighScore = jedis.get(userHighScoreKey);
            if (userHighScore != null) {
                jedis.zadd("highscores", Integer.parseInt(userHighScore), "user" + ":" + i);
            }
        }
        System.out.println("Updated overall high scores in " + (System.currentTimeMillis() - start) + " ms");
    }

    // Some example queries to get scores
    public void getScores() {
        long start = System.currentTimeMillis();
        System.out.println("Top-100");
        Set<Tuple> scores = jedis.zrevrangeByScoreWithScores("highscores",0,100);
        for (Tuple score : scores) {
            System.out.println(score.getElement() + " : " + score.getScore());
        }
        System.out.println("Rank of user 1000 " + jedis.zrank("highscores", "user:1000"));
        System.out.println("Executed rank queries in " + (System.currentTimeMillis() - start) + " ms");
    }

    public static void main(String[] args) {
        RedisExample example = new RedisExample();
        // Import user data
        example.importUserData();
        example.importUserScores();
        example.updateUserHighScores();
        example.updateHighScores();
        example.getScores();
    }

}
