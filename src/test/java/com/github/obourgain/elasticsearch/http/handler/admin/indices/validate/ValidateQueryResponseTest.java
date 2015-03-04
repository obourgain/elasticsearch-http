package com.github.obourgain.elasticsearch.http.handler.admin.indices.validate;

import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.validate.ValidateQueryResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

public class ValidateQueryResponseTest {

    @Test
    public void valid_query() throws Exception {
        String json = "{\"valid\":true,\"_shards\":{\"total\":3,\"successful\":1,\"failed\":2}}";
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        ValidateQueryResponse response = new ValidateQueryResponse().parse(new BytesArray(json.getBytes()));

        Shards shards = response.getShards();
        Assertions.assertThat(shards.getTotal()).isEqualTo(3);
        Assertions.assertThat(shards.getSuccessful()).isEqualTo(1);
        Assertions.assertThat(shards.getFailed()).isEqualTo(2);

        Assertions.assertThat(response.isValid()).isTrue();
    }

    @Test
    public void invalid_query() throws Exception {
        String json = "{\"valid\":false,\"_shards\":{\"total\":3,\"successful\":1,\"failed\":2}}";
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        ValidateQueryResponse response = new ValidateQueryResponse().parse(new BytesArray(json.getBytes()));

        Shards shards = response.getShards();
        Assertions.assertThat(shards.getTotal()).isEqualTo(3);
        Assertions.assertThat(shards.getSuccessful()).isEqualTo(1);
        Assertions.assertThat(shards.getFailed()).isEqualTo(2);

        Assertions.assertThat(response.isValid()).isFalse();
    }
}