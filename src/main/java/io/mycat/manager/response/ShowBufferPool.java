package io.mycat.manager.response;

import java.nio.ByteBuffer;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.DirectByteBufferPool;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.StringUtil;

/**
 * 显示当前bufferpool使用情况(Note: 目前仅支持DirectByteBufferPool的状态显示)
 * @author CrazyPig
 *
 */
public class ShowBufferPool {
    
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil
            .getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("BUFFER_POOL_STATUS",
                Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        eof.packetId = ++packetId;
    }
    
    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = header.write(buffer, c, true);
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }
        buffer = eof.write(buffer, c, true);
        byte packetId = eof.packetId;
        String charset = c.getCharset();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.packetId = ++packetId;
        BufferPool bufferPool = c.getProcessor().getBufferPool();
        if (bufferPool instanceof DirectByteBufferPool) {
            DirectByteBufferPool directBufferPool = (DirectByteBufferPool) bufferPool;
            row.add(StringUtil.encode(directBufferPool.toString(), charset));
        } else {
            row.add(StringUtil.encode("Not yet support buffer pool type of " + bufferPool.getClass().getSimpleName(), charset));
        }
        buffer = row.write(buffer, c, true);
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);
        c.write(buffer);
    }

}
