package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import sun.nio.ch.DirectBuffer;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 * @author wuzhih
 * @author zagnix
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool implements BufferPool{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufferPool.class);
    public static final String LOCAL_BUF_THREAD_PREX = "$_";
    private ByteBufferPage[] allPages;
    private final int chunkSize;
   // private int prevAllocatedPage = 0;
    private AtomicInteger prevAllocatedPage;
    private final  int pageSize;
    private final short pageCount;
    private final int conReadBuferChunk ;
    /**
     * 记录对线程ID->该线程的所使用Direct Buffer的size
     */
    private final ConcurrentHashMap<Object,Long> memoryUsage;
    
    private AtomicLong allocCnt = new AtomicLong(0L);
    private AtomicLong allocSuccessCnt = new AtomicLong(0L);
    private AtomicLong recycleCnt = new AtomicLong(0L);
    private AtomicLong recycleSuccessCnt = new AtomicLong(0L);

    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount,int conReadBuferChunk) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.conReadBuferChunk = conReadBuferChunk;
        prevAllocatedPage = new AtomicInteger(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
        memoryUsage = new ConcurrentHashMap<>();
    }

    public BufferArray allocateArray() {
        return new BufferArray(this);
    }
    /**
     * TODO 当页不够时，考虑扩展内存池的页的数量...........
     * @param buffer
     * @return
     */
    public  ByteBuffer expandBuffer(ByteBuffer buffer){
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity << 1;
        ByteBuffer newBuffer = allocate(newCapacity);
        if(newBuffer != null){
            int newPosition = buffer.position();
            buffer.flip();
            newBuffer.put(buffer);
            newBuffer.position(newPosition);
            recycle(buffer);
            return  newBuffer;
        }
        return null;
    }

    public ByteBuffer allocate(int size) {
    	allocCnt.incrementAndGet();
       final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage =  prevAllocatedPage.incrementAndGet() % allPages.length;
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        final String threadName = Thread.currentThread().getName();

        if(byteBuf !=null){
        	allocSuccessCnt.incrementAndGet();
            if (memoryUsage.containsKey(threadName)){
                memoryUsage.put(threadName,memoryUsage.get(threadName)+byteBuf.capacity());
            }else {
                memoryUsage.put(threadName,(long)byteBuf.capacity());
            }
        } else {
        	LOGGER.warn("can not allocate bytebuffer from pool");
        }
        return byteBuf;
    }

    public void recycle(ByteBuffer theBuf) {
    	recycleCnt.incrementAndGet();
    	if(!(theBuf instanceof DirectBuffer)){
    		LOGGER.warn("can not recycle non direct bytebuffer");
    		theBuf.clear();
    		return;
    	}

    	final long size = theBuf.capacity();

        boolean recycled = false;
        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        int chunkCount = theBuf.capacity() / chunkSize;
        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
        for (int i = 0; i < allPages.length; i++) {
            if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true)) {
                break;
            }
        }
        final String threadName = Thread.currentThread().getName();

        if (memoryUsage.containsKey(threadName)){
            memoryUsage.put(threadName,memoryUsage.get(threadName)-size);
        }
        if (recycled == false) {
            LOGGER.warn("warning ,not recycled buffer " + theBuf);
        } else {
        	recycleSuccessCnt.incrementAndGet();
        }
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }
	
	 @Override
    public ConcurrentHashMap<Object,Long> getNetDirectMemoryUsage() {
        return memoryUsage;
    }

    public int getPageSize() {
        return pageSize;
    }

    public short getPageCount() {
        return pageCount;
    }

    //TODO   should  fix it
    public long capacity(){
        return size();
    }

    public long size(){
        return  (long) pageSize * chunkSize * pageCount;
    }

    //TODO
    public  int getSharedOptsCount(){
        return 0;
    }


    public int getConReadBuferChunk() {
        return conReadBuferChunk;
    }
    
    public ByteBufferPage[] getByteBufferPage() {
    	return allPages;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final String LINE_SEP = System.getProperty("line.separator");
        sb.append("DirectByteBufferPool status : {" + LINE_SEP)
                .append(" allocCnt = " + this.allocCnt.get())
                .append(", allocSuccessCnt = " + this.allocSuccessCnt.get())
                .append(", recycleCnt = " + this.recycleCnt.get())
                .append(", recycleSuccessCnt = " + this.recycleSuccessCnt.get()).append(LINE_SEP);
        for (int i = 0; i < allPages.length; i++) {
            ByteBufferPage onePage = allPages[i];
            sb.append("page " + i + " : " + onePage.toString() + LINE_SEP);
        }
        sb.append("}");
        return sb.toString();
    }

}
