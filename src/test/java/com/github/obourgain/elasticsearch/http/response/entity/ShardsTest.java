package com.github.obourgain.elasticsearch.http.response.entity;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class ShardsTest {

    @Test
    public void testParse() throws Exception {
        String json = "{\"total\":3,\"successful\":3,\"failed\":0}}}";
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        Shards shards = new Shards().parse(parser);

        assertThat(shards.getTotal()).isEqualTo(3);
        assertThat(shards.getSuccessful()).isEqualTo(3);
        assertThat(shards.getFailed()).isEqualTo(0);
    }
}