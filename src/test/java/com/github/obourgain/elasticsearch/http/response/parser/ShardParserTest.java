package com.github.obourgain.elasticsearch.http.response.parser;

import static org.assertj.core.api.Assertions.*;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

public class ShardParserTest {

    @Test
    public void testParse() throws Exception {
        String json = "{\"_shards\":{\"total\":3,\"successful\":3,\"failed\":0}}}}";
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        Shards shards = ShardParser.parse(parser);

        assertThat(shards.getTotal()).isEqualTo(3);
        assertThat(shards.getSuccessful()).isEqualTo(3);
        assertThat(shards.getFailed()).isEqualTo(0);

    }
}