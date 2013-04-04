package bolts;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">tigerby</a>
 * @version 1.0
 */
public class UserHashtagJoinBolt extends BaseBatchBolt {
    BatchOutputCollector collector;

    private Map<String, String> userTweets;
    private Map<String, String> tweetHashtags;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector batchOutputCollector, Object o) {
        this.collector = batchOutputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String source = tuple.getSourceStreamId();
        String tweetId = tuple.getStringByField("tweet_id");

        if("hashtags".equals(source)) {
            String hashtag = tuple.getStringByField("hashtag");
            add(tweetHashtags, tweetId, hashtag);
        } else if("users".equals(source)) {
            String user = tuple.getStringByField("user");
            add(userTweets, user, tweetId);
        }
    }

    @Override
    public void finishBatch() {
        for (String user : userTweets.keySet()) {
            Set<String> tweets = getUserTweets(user);
            HashMap<String, Integer> hashtagsCounter = new HashMap<String, Integer>();
            for (String tweet : tweets) {
                Set<String> hashtags = getTweetHashtags(tweet);
                if(hashtags != null) {
                    for (String hashtag : hashtags) {
                        Integer count = hashtagsCounter.get(hashtag);
                        if(count == null)
                            count = 0;
                        count ++;
                        hashtagsCounter.put(hashtag, count);
                    }
                }
            }

            for (String hashtag : hashtagsCounter.keySet()) {
                int count = hashtagsCounter.get(hashtag);
                collector.emit(new Values(id, user, hashtag, count));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private void add(Map<String, String> targetMap, String tweetId, String data) {
        targetMap.put(tweetId, data);

    }

    private Set<String> getUserTweets(String user) {
        return null;
    }

    private Set<String> getTweetHashtags(String tweet) {
        return null;
    }
}
