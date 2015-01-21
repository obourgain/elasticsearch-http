package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.nio.ByteBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.ning.http.client.Body;

public class BulkBody implements Body {

    private BulkActionMarshaller marshaller;

    // after a line have been written, before writing the next
    @VisibleForTesting protected boolean writeEndOfLine;

    @VisibleForTesting protected byte[] current;
    @VisibleForTesting protected int currentPosition;

    public BulkBody(BulkActionMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public long read(ByteBuffer target) throws IOException {
        byte[] source;
        int position;
        if (current != null) {
            source = current;
            position = currentPosition;
            current = null;
            currentPosition = 0;
        } else if(writeEndOfLine) {
            writeEndOfLine = false;
            source = "\n".getBytes(Charsets.US_ASCII);
            position = 0;
        } else {
            byte[] next = marshaller.next();
            if(next == null) {
                return -1; // nothing more to write
            }
            source = next;
            position = 0;
        }

        int remainingInTarget = target.remaining();
        int remainingInSource = source.length - position; // TODO off by one ?

        int toWrite = Math.min(remainingInSource, remainingInTarget);
        int endPosition = position + toWrite;
        for (int i = position; i < endPosition; i++) {
            target.put(source[i]);
        }
        if (endPosition != source.length) {
            current = source;
            currentPosition = endPosition;
        } else {
            writeEndOfLine = true;
        }

        return toWrite;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public void close() throws IOException {

    }
}
