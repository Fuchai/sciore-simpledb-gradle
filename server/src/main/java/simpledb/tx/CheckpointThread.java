package simpledb.tx;

import simpledb.server.SimpleDB;
import simpledb.tx.recovery.CheckpointRecord;

public class CheckpointThread implements Runnable {

    static boolean inProgress=false;
    static boolean transactionLockacquired =false;
    static Object checkpointLock = new Object();

//    private static Lock checkpointLock = new ReentrantLock();
//    private static Condition noActive= checkpointLock.newCondition();

    public void run(){
        inProgress=true;
        synchronized (checkpointLock){
            while (!Transaction.getCurrentlyActiveTransactions().isEmpty()){
                try {
                    checkpointLock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Flush all is automatic if we wait for all the transactions to finish
                // When commit or rollback happen

                int lsn = new CheckpointRecord().writeToLog();
                SimpleDB.logMgr().flush(lsn);

                inProgress=false;
                synchronized (Transaction.getTransactionLock()) {
                    Transaction.getTransactionLock().notifyAll();
                }
            }
        }
    }

//    public static Condition getNoActive() {
//        return noActive;
//    }

    public static boolean isInProgress() {
        return inProgress;
    }

    public static Object getCheckpointLock() {
        return checkpointLock;
    }


    public static void setInProgress(boolean inProgress) {
        CheckpointThread.inProgress = inProgress;
    }
}
