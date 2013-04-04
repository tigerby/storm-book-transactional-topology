package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">tigerby</a>
 * @version 1.0
 */
public class UserSplitterBolt implements IBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("users", new Fields("txid", "tweet_id", "user"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) { }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String tweet = input.getStringByField("tweet");
        String tweetId = input.getStringByField("tweet_id");
        StringTokenizer strTok = new StringTokenizer(tweet, " ");
        TransactionAttempt tx = (TransactionAttempt)input.getValueByField("txid");
        HashSet<String> users = new HashSet<String>();

        while(strTok.hasMoreTokens()) {
            String user = strTok.nextToken();
            // Ensure this is an actual user, and that it's not repeated in the tweet
            if(user.startsWith("@") && !users.contains(user)) {
                collector.emit("users", new Values(tx, tweetId, user));
                users.add(user);
            }
        }
    }

    @Override
    public void cleanup() { }
}
