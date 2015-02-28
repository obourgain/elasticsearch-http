package com.github.obourgain.elasticsearch.http.response.entity;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class MappingMetaDataTest {

    @Test
    public void should_parse_type() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/mappings/get/type_mapping.json");

        XContentParser parser = XContentHelper.createParser(new BytesArray(json));
        parser.nextToken();

        MappingMetaData metaData = new MappingMetaData().parse(parser);

        Map<String, Object> map = metaData.getAsMap();

        Assertions.assertThat(new Object());

        assertThat(map).hasSize(1).containsKey("properties");
        assertThat((Map<String, Object>) map.get("properties")).isInstanceOf(Map.class)
                .hasSize(3)
                .containsKey("message")
                .containsKey("post_date")
                .containsKey("user");
    }

}