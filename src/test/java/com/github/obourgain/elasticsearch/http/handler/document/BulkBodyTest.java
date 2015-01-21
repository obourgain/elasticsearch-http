package com.github.obourgain.elasticsearch.http.handler.document;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;
import static org.mockito.Mockito.verify;
import java.nio.ByteBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class BulkBodyTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Test
    public void should_ask_more_data() throws Exception {
        BulkActionMarshaller marshaller = mock(BulkActionMarshaller.class);
        BulkBody bulkBody = new BulkBody(marshaller);

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        bulkBody.read(buffer);

        verify(marshaller).next();
    }

    @Test
    public void should_return_minus_one_when_done() throws Exception {
        BulkActionMarshaller marshaller = mock(BulkActionMarshaller.class);
        BulkBody bulkBody = new BulkBody(marshaller);

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        long read = bulkBody.read(buffer);

        assertThat(read).isEqualTo(-1);
        verify(marshaller).next();
    }

    @Test
    public void should_write_fully_if_target_have_enough_room() throws Exception {
        byte[] bytes = new byte[1024];
        bytes[1023] = 42;
        BulkActionMarshaller marshaller = mock(BulkActionMarshaller.class);
        given(marshaller.next()).willReturn(bytes);
        BulkBody bulkBody = new BulkBody(marshaller);

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        long read = bulkBody.read(buffer);

        assertThat(read).isEqualTo(1024);
        assertThat(buffer.get(1023)).isEqualTo((byte) 42);
    }

    @Test
    public void should_keep_current_if_target_cant_write_all() throws Exception {
        byte[] bytes = new byte[1024];
        bytes[1023] = 42;
        BulkActionMarshaller marshaller = mock(BulkActionMarshaller.class);
        given(marshaller.next()).willReturn(bytes);
        BulkBody bulkBody = new BulkBody(marshaller);

        ByteBuffer buffer = ByteBuffer.allocate(512);
        long read = bulkBody.read(buffer);

        assertThat(read).isEqualTo(512);
        assertThat(buffer.remaining()).isEqualTo(0);
        assertThat(bulkBody.current).isSameAs(bytes);
        assertThat(bulkBody.currentPosition).isEqualTo(512);
    }

    @Test
    public void should_keep_current_if_target_cant_write_all_several_times() throws Exception {
        byte[] bytes = new byte[1024];
        bytes[1023] = 42;
        BulkActionMarshaller marshaller = mock(BulkActionMarshaller.class);
        given(marshaller.next()).willReturn(bytes);
        BulkBody bulkBody = new BulkBody(marshaller);

        ByteBuffer buffer = ByteBuffer.allocate(16);
        long read = bulkBody.read(buffer);

        assertThat(read).isEqualTo(16);
        assertThat(buffer.remaining()).isEqualTo(0);
        assertThat(bulkBody.current).isSameAs(bytes);
        assertThat(bulkBody.currentPosition).isEqualTo(16);

        buffer.rewind();
        read = bulkBody.read(buffer);

        assertThat(read).isEqualTo(16);
        assertThat(buffer.remaining()).isEqualTo(0);
        assertThat(bulkBody.current).isSameAs(bytes);
        assertThat(bulkBody.currentPosition).isEqualTo(32);
    }
}