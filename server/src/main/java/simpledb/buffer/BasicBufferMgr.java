package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.FileMgr;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
    private Buffer[] bufferpool;
    private int numAvailable;
    private int strategy;
    private int numbuffs;
    private Queue<Buffer> FIFO;
    private Queue<Buffer> LRU;
    private int clockHand =0;

    /**
     * Creates a buffer manager having the specified number
     * of buffer slots.
     * This constructor depends on both the {@link FileMgr} and
     * {@link simpledb.log.LogMgr LogMgr} objects
     * that it gets from the class
     * {@link simpledb.server.SimpleDB}.
     * Those objects are created during system initialization.
     * Thus this constructor cannot be called until
     * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
     * is called first.
     * @param numbuffs the number of buffer slots to allocate
     */
    BasicBufferMgr(int numbuffs) {
        this.numbuffs=numbuffs;
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        FIFO = new LinkedList<>();
        for (int i=0; i<numbuffs; i++) {
            Buffer buff=new Buffer();
            bufferpool[i] = buff;
            FIFO.add(buff);
        }
        LRU= new LinkedList(Arrays.asList(bufferpool));
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * @param txnum the transaction's id number
     */
    synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferpool)
            if (buff.isModifiedBy(txnum))
                buff.flush();
    }

    /**
     * Pins a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     * @param blk a reference to a disk block
     * @return the pinned buffer
     */
    synchronized Buffer pin(Block blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            buff.assignToBlock(blk);
        }
        if (!buff.isPinned()){
            FIFO.remove(buff);
            FIFO.add(buff);
            numAvailable--;
        }else{
            FIFO.remove(buff);
            FIFO.add(buff);
        }
        buff.pin();
        return buff;
    }

    /**
     * Allocates a new block in the specified file, and
     * pins a buffer to it.
     * Returns null (without allocating the block) if
     * there are no available buffers.
     * @param filename the name of the file
     * @param fmtr a pageformatter object, used to format the new block
     * @return the pinned buffer
     */
    synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
        Buffer buff = chooseUnpinnedBuffer();
        if (buff == null)
            return null;
        buff.assignToNew(filename, fmtr);
        numAvailable--;
        FIFO.remove(buff);
        FIFO.add(buff);
        buff.pin();
        return buff;
    }

    /**
     * Unpins the specified buffer.
     * @param buff the buffer to be unpinned
     */
    synchronized void unpin(Buffer buff) {
        buff.unpin();
        LRU.remove(buff);
        LRU.add(buff);
        if (!buff.isPinned()){
            numAvailable++;
        }
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     * @return the number of available buffers
     */
    int available() {
        return numAvailable;
    }

    private Buffer findExistingBuffer(Block blk) {
        for (Buffer buff : bufferpool) {
            Block b = buff.block();
            if (b != null && b.equals(blk))
                return buff;
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        switch (this.strategy) {
            case 0:
                return useNaiveStrategy();
            case 1:
                return useFIFOStrategy();
            case 2:
                return useLRUStrategy();
            case 3:
                return useClockStrategy();
            default:
                return null;
        }
    }
    /**
     * @return Allocated buffers
     */
    public Buffer[] getBuffers() {
        return this.bufferpool;
    }
    /**
     * Set buffer selection strategy
     * @param s (0 - Naive, 1 - FIFO, 2 - LRU, 3 - Clock)
     */
    public void setStrategy(int s) {
        this.strategy = s;
    }
    /**
     * Naive buffer selection strategy
     * @return
     */
    private Buffer useNaiveStrategy() {
        for (Buffer buff : bufferpool)
            if (!buff.isPinned())
                return buff;
        return null;
    }
    /**
     * FIFO buffer selection strategy
     * First buffer that gets pinned will be first out. Note that buffer that gets
     * pinned does not become available. When a buffer gets freed, the date when
     * it gets pinned then becomes significant.
     * @return
     */
    private Buffer useFIFOStrategy() {
        Iterator<Buffer> it=FIFO.iterator();
        while(it.hasNext()){
            Buffer buff=it.next();
            if (!buff.isPinned()){
                it.remove();
                return buff;
            }
        }
        return null;
    }

    /**
     * LRU buffer selection strategy
     * @return
     */
    private Buffer useLRUStrategy() {
        return LRU.remove();
    }
    /**
     * Clock buffer selection strategy
     * @return
     */
    private Buffer useClockStrategy() {
        int start= clockHand;
        do {
            if (!bufferpool[clockHand].isPinned()){
                return bufferpool[clockHand];
            }

            if (clockHand <this.numbuffs-1){
                clockHand++;
            }else {
                clockHand =0;
            }
        } while(clockHand !=start);
        return null;
    }

    public void setClockHand(int clockHand) {
        this.clockHand = clockHand;
    }
}
