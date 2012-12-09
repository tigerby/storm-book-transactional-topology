package spouts;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">tigerby</a>
 * @version 1.0
 */
public class TransactionMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    long from;
    int quantity;

    public TransactionMetadata(long from, int quantity) {
        this.from = from;
        this.quantity = quantity;
    }
}