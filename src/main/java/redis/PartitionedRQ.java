package redis;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * <p> title here </p>
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class PartitionedRQ {
    public static final String NEXT_READ = "NEXT_READ";
    public static final String NEXT_WRITE = "NEXT_WRITE";

    Jedis jedis;

    public PartitionedRQ() {
        jedis = new Jedis("localhost");
    }

    public long getAvailableToRead(int partition, long current) {
//        return getNextWrite() - current;
        return -1;
    }

    public long getNextRead(int partition) {
//        String sNextRead = jedis.get(NEXT_READ);
//
//        if(sNextRead == null)
//            return 1;
//
//        return Long.valueOf(sNextRead);
        return -1;
    }

    public long getNextWrite() {
//        return Long.valueOf(jedis.get(NEXT_WRITE));
        return -1;
    }

    public void close() {
        jedis.disconnect();
    }

    public void setNextRead(int partition, long nextRead) {
//        jedis.set(NEXT_READ, ""+nextRead);
    }

    public List<String> getMessages(int partition, long from, int quantity) {
//        String[] keys = new String[quantity];
//        for (int i = 0; i < quantity; i++)
//            keys[i] = ""+(i+from);
//
//        return jedis.mget(keys);
        return null;
    }
}
