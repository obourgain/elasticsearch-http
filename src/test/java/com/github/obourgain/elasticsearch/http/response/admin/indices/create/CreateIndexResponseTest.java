package com.github.obourgain.elasticsearch.http.response.admin.indices.create;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;

public class CreateIndexResponseTest {

    @Test
    public void should_parse_succes() {
        String json = "{ \"acknowledged\": true }";
        CreateIndexResponse response = CreateIndexResponse.doParse(new BytesArray(json), 200);
        assertThat(response.isAcknowledged()).isTrue();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getError()).isNull();
    }

    @Test
    public void should_parse_failure() {
        String json = "{\n" +
                "   \"error\": \"IndexAlreadyExistsException[[twitter2] already exists]\",\n" +
                "   \"status\": 400\n" +
                "}";
        CreateIndexResponse response = CreateIndexResponse.doParse(new BytesArray(json), 400);
        assertThat(response.isAcknowledged()).isFalse();
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getError()).isEqualToIgnoringCase("IndexAlreadyExistsException[[twitter2] already exists]");
    }

}