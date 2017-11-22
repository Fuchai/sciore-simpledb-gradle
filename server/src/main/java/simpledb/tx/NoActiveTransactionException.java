package simpledb.tx;

public class NoActiveTransactionException extends Exception {
    NoActiveTransactionException(String s){
        super(s);
    }
}
