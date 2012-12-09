package spouts;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Values;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Bryan Kim</a>
 * @version 1.0
 */
public class TweetsPartitionedTransactionalSpout extends BasePartitionedTransactionalSpout<TransactionMetadata> {
    public static class TweetsPartitionedTransactionalCoordinator implements Coordinator {
        @Override
        public int numPartitions() {
            return 4;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    public static class TweetsPartitionedTransactionalEmitter implements Emitter<TransactionMetadata> {
        PartitionedRQ rq = new PartitionedRQ();

        @Override
        public TransactionMetadata emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, TransactionMetadata lastPartitionMeta) {
            long nextRead;
            if(lastPartitionMeta == null)
                nextRead = rq.getNextRead(partition);
            else {
                nextRead = lastPartitionMeta.from + lastPartitionMeta.quantity;
                rq.setNextRead(partition, nextRead); // Move the cursor
            }

            long quantity = rq.getAvailableToRead(partition, nextRead);
            quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
            TransactionMetadata metadata = new TransactionMetadata(nextRead, (int)quantity);
            emitPartitionBatch(tx, collector, partition, metadata);
            return metadata;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, TransactionMetadata partitionMeta) {
            if(partitionMeta.quantity <= 0)
                return ;

            List<String> messages = rq.getMessages(partition, partitionMeta.from, partitionMeta.quantity);
            long tweetId = partitionMeta.from;
            for (String msg : messages) {
                collector.emit(new Values(tx, ""+tweetId, msg));
                tweetId ++;
            }
        }

        @Override
        public void close() {
        }
    }

    public static class TweetsOpaquePartitionedTransactionalSpoutCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
        @Override
        public boolean isReady() {
            return true;
        }
    }

    public static class TweetsOpaquePartitionedTransactionalSpoutEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<TransactionMetadata> {
        PartitionedRQ rq = new PartitionedRQ();

        @Override
        public TransactionMetadata emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, TransactionMetadata lastPartitionMeta) {
            long nextRead;
            if(lastPartitionMeta == null)
                nextRead = rq.getNextRead(partition);
            else {
                nextRead = lastPartitionMeta.from + lastPartitionMeta.quantity;
                rq.setNextRead(partition, nextRead); // Move the cursor
            }

            long quantity = rq.getAvailableToRead(partition, nextRead);
            quantity = quantity > MAX_TRANSACTION_SIZE ? MAX_TRANSACTION_SIZE : quantity;
            TransactionMetadata metadata = new TransactionMetadata(nextRead, (int)quantity);
            emitMessages(tx, collector, partition, metadata);
            return metadata;
        }

        private void emitMessages(TransactionAttempt tx, BatchOutputCollector collector, int partition, TransactionMetadata partitionMeta) {
            if(partitionMeta.quantity <= 0) return ;
            List<String> messages =
                    rq.getMessages(partition, partitionMeta.from, partitionMeta.quantity);
            long tweetId = partitionMeta.from; for (String msg : messages) {
                collector.emit(new Values(tx, ""+tweetId, msg));
                tweetId ++;
            }
        }

        @Override
        public int numPartitions() {
            return 4;
        }

        @Override
        public void close() {
        }
    }
}
