package redis;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">tigerby</a>
 * @version 1.0
 */
public class RQ {
    public static final String NEXT_READ = "NEXT_READ";
    public static final String NEXT_WRITE = "NEXT_WRITE";

    Jedis jedis;

    public RQ() {
        jedis = new Jedis("localhost");
    }

    public long getAvailableToRead(long current) {
        return getNextWrite() - current;
    }

    public long getNextRead() {
        String sNextRead = jedis.get(NEXT_READ);

        if(sNextRead == null)
            return 1;

        return Long.valueOf(sNextRead);
    }

    public long getNextWrite() {
        return Long.valueOf(jedis.get(NEXT_WRITE));
    }

    public void close() {
        jedis.disconnect();
    }

    public void setNextRead(long nextRead) {
        jedis.set(NEXT_READ, ""+nextRead);
    }

    public List<String> getMessages(long from, int quantity) {
        String[] keys = new String[quantity];
        for (int i = 0; i < quantity; i++)
            keys[i] = ""+(i+from);

        return jedis.mget(keys);
    }
}