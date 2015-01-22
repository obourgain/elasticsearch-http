package com.github.obourgain.elasticsearch.http.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import io.netty.buffer.ByteBuf;

public class ByteBufBytesReference implements BytesReference {

    private final ByteBuf buf;

    private int start;
    private int end;

    public ByteBufBytesReference(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public byte get(int index) {
        return buf.getByte(index + start);
    }

    @Override
    public int length() {
        return 0;
    }

    @Override
    public BytesReference slice(int from, int length) {
        return null;
    }

    @Override
    public StreamInput streamInput() {
        return null;
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {

    }

    @Override
    public void writeTo(GatheringByteChannel channel) throws IOException {

    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }

    @Override
    public BytesArray toBytesArray() {
        return null;
    }

    @Override
    public BytesArray copyBytesArray() {
        return null;
    }

    @Override
    public ChannelBuffer toChannelBuffer() {
        return null;
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        return new byte[0];
    }

    @Override
    public int arrayOffset() {
        return 0;
    }

    @Override
    public String toUtf8() {
        return null;
    }

    @Override
    public BytesRef toBytesRef() {
        return null;
    }

    @Override
    public BytesRef copyBytesRef() {
        return null;
    }
}
