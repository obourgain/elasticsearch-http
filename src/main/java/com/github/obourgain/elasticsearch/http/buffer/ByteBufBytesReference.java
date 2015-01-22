package com.github.obourgain.elasticsearch.http.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.netty.buffer.ByteBufferBackedChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;

public class ByteBufBytesReference implements BytesReference {

    private final ByteBuf buf;

    public ByteBufBytesReference(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public byte get(int index) {
        return buf.getByte(index);
    }

    @Override
    public int length() {
        return buf.capacity();
    }

    @Override
    public BytesReference slice(int from, int length) {
        return new ByteBufBytesReference(buf.slice(from, length));
    }

    @Override
    public StreamInput streamInput() {
        return new BytesStreamInput(this);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        if(buf.hasArray()) {
            os.write(buf.array());
        } else {
            byte[] bytes = new byte[buf.capacity()];
            buf.duplicate().getBytes(0, bytes);
            os.write(bytes);
        }
    }

    @Override
    public void writeTo(GatheringByteChannel channel) throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public byte[] toBytes() {
        return Arrays.copyOf(buf.array(), buf.capacity());
    }

    @Override
    public BytesArray toBytesArray() {
        return new BytesArray(this.toBytesRef());
    }

    @Override
    public BytesArray copyBytesArray() {
        return new BytesArray(this.copyBytesRef());
    }

    @Override
    public ChannelBuffer toChannelBuffer() {
        return new ByteBufferBackedChannelBuffer(buf.nioBuffer());
    }

    @Override
    public boolean hasArray() {
        return buf.hasArray();
    }

    @Override
    public byte[] array() {
        if(buf.hasArray()) {
            return buf.array();
        } else {
            byte[] bytes = new byte[buf.capacity()];
            buf.duplicate().getBytes(0, bytes);
            return bytes;
        }
    }

    @Override
    public int arrayOffset() {
        return buf.readerIndex();
    }

    @Override
    public String toUtf8() {
        if (buf.capacity() == 0) {
            return "";
        }
        return buf.toString(Charsets.UTF_8);
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(array(), 0, buf.capacity());
    }

    @Override
    public BytesRef copyBytesRef() {
        return new BytesRef(toBytes(), 0, buf.capacity());
    }
}
