package edu.uci.ics.texera.perftest.twitter;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class TwitterSample {
    
    public static String twitterFilePath = PerfTestUtils.getResourcePath("/sample-data-files/twitter/tweets.json").toString();
    public static String twitterClimateTable = "twitter_sample";
    
    public static void main(String[] args) throws Exception {
        writeTwitterIndex();
    }
    
    public static void writeTwitterIndex() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(twitterClimateTable);
        relationManager.createTable(twitterClimateTable, Utils.getDefaultIndexDirectory().resolve(twitterClimateTable), TwitterSchema.TWITTER_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(twitterClimateTable);
        dataWriter.open();
        
        JsonNode jsonNode = new ObjectMapper().readTree(new File(twitterFilePath));
        for (JsonNode tweet : jsonNode) {
            try {
                String text = tweet.get("text").asText();
                Long id = tweet.get("id").asLong();
                String tweetLink = "https://twitter.com/statuses/" + id;
                JsonNode userNode = tweet.get("user");
                String userScreenName = userNode.get("screen_name").asText();
                String userLink = "https://twitter.com/" + userScreenName;
                String userName = userNode.get("name").asText();
                String userDescription = userNode.get("description").asText();
                Integer userFollowersCount = userNode.get("followers_count").asInt();
                Integer userFriendsCount = userNode.get("friends_count").asInt();
                JsonNode geoTagNode = tweet.get("geo_tag");
                String state = geoTagNode.get("stateName").asText();
                String county = geoTagNode.get("countyName").asText();
                String city = geoTagNode.get("cityName").asText();
                String createAt = tweet.get("create_at").asText();
                Tuple tuple = new Tuple(TwitterSchema.TWITTER_SCHEMA,
                        new TextField(text),
                        new StringField(tweetLink),
                        new StringField(userLink),
                        new TextField(userScreenName),
                        new TextField(userName),
                        new TextField(userDescription),
                        new IntegerField(userFollowersCount),
                        new IntegerField(userFriendsCount),
                        new TextField(state),
                        new TextField(county),
                        new TextField(city),
                        new StringField(createAt));
                dataWriter.insertTuple(tuple);
            } catch (Exception e) {
                // catch all exception (including NullPointerException)
                // continue to next tuple if something goes wrong
                continue;
            }
        }
        
        dataWriter.close();
    }

}
