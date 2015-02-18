package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class CardinalityTest {

    @Test
    public void should_parse_min() throws IOException {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/cardinality/cardinality.json");
        Min min = Min.parse(XContentHelper.createParser(new BytesArray(json)), "author_count");

        assertThat(min.getValue()).isEqualTo(12);
    }
}