package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;

/**
 * Created by Andres on 3/31/17.
 */
public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            if (tweet.contains("MAGA")){
                context.write(new Text("MAGA"), new Text(String.valueOf(status.getId())));
            }
            if (tweet.contains("DICTATOR")){
                context.write(new Text("DICTATOR"), new Text(String.valueOf(status.getId())));
            }
            if (tweet.contains("IMPEACH")){
                context.write(new Text("IMPEACH"), new Text(String.valueOf(status.getId())));
            }
            if (tweet.contains("DRAIN")){
                context.write(new Text("DRAIN"), new Text(String.valueOf(status.getId())));
            }
            if (tweet.contains("SWAMP")){
                context.write(new Text("SWAMP"), new Text(String.valueOf(status.getId())));
            }
            if (tweet.contains("CHANGE")){
                context.write(new Text("CHANGE"), new Text(String.valueOf(status.getId())));
            }
        }

        catch(TwitterException e){

        }

    }
}
