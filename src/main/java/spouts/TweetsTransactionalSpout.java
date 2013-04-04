package spouts;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.RQ;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Bryan Kim</a>
 * @version 1.0
 */
public class TweetsTransactionalSpout extends BaseTransactionalSpout<TransactionMetadata> {
    @Override
    public ITransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(Map conf, TopologyContext context) {
        return new TweetsTransactionalSpoutCoordinator();
    }

    @Override
    public backtype.storm.transactional.ITransactionalSpout.Emitter<TransactionMetadata> getEmitter(
            Map conf, TopologyContext context) {
        return new TweetsTransactionalSpoutEmitter();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("txid", "tweet_id", "tweet"));
    }

    public static class TweetsTransactionalSpoutCoordinator implements ITransactionalSpout.Coordinator<TransactionMetadata> {
        private static final long MAX_TRANSACTION_SIZE = 100;

        TransactionMetadata lastTransactionMetadata;
        RQ rq = new RQ();
        long nextRead = 0;

        public TweetsTransactionalSpoutCoordinator() {
            nextRead = rq.getNextRead();
        }

        @Override
        public TransactionMetadata initializeTransaction(BigInteger txid, TransactionMetadata prevMetadata) {
            long quantity = rq.getAvailableToRead(nextRead);
            quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
            TransactionMetadata ret = new TransactionMetadata(nextRead, (int)quantity);
            nextRead += quantity;
            return ret;
        }

        @Override
        public boolean isReady() {
            return rq.getAvailableToRead(nextRead) > 0;
        }

        @Override
        public void close() {
            rq.close();
        }
    }

    public static class TweetsTransactionalSpoutEmitter implements ITransactionalSpout.Emitter<TransactionMetadata> {
        RQ rq = new RQ();

        public TweetsTransactionalSpoutEmitter() {
        }


        @Override
        public void emitBatch(TransactionAttempt tx, TransactionMetadata coordinatorMeta, BatchOutputCollector collector) {
            rq.setNextRead(coordinatorMeta.from+coordinatorMeta.quantity);
            List<String> messages = rq.getMessages(coordinatorMeta.from, coordinatorMeta.quantity);

            long tweetId = coordinatorMeta.from;

            for (String message : messages) {
                collector.emit(new Values(tx, ""+tweetId, message));
                tweetId++;
            }
        }

        @Override
        public void cleanupBefore(BigInteger txid) {
        }

        @Override
        public void close() {
            rq.close();
        }
    }

}
