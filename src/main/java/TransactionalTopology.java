import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.HashtagSplitterBolt;
import bolts.RedisCommiterBolt;
import bolts.UserHashtagJoinBolt;
import bolts.UserSplitterBolt;
import spouts.TweetsTransactionalSpout;

/**
 * Created with IntelliJ IDEA.
 * @author <a href="mailto:bongyeonkim@gmail.com">Bryan Kim</a>
 * @version 1.0
 */

public class TransactionalTopology {
    public static void main(String[] args) {
        TransactionalTopologyBuilder builder =
                new TransactionalTopologyBuilder("test", "spout", new TweetsTransactionalSpout());

        builder.setBolt("users-splitter", new UserSplitterBolt(), 4)
                .shuffleGrouping("spout");

        builder.setBolt("hashtag-splitter", new HashtagSplitterBolt(), 4)
                .shuffleGrouping("spout");

        builder.setBolt("user-hashtag-merger", new UserHashtagJoinBolt(), 4)
                .fieldsGrouping("users-splitter","users", new Fields("tweet_id"))
                .fieldsGrouping("hashtag-splitter", "hashtags", new Fields("tweet_id"));

        builder.setBolt("redis-committer", new RedisCommiterBolt())
                .globalGrouping("users-splitter","users")
                .globalGrouping("hashtag-splitter", "hashtags")
                .globalGrouping("user-hashtag-merger");
    }
}
