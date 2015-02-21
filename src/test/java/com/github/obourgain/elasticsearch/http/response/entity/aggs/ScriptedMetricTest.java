package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class ScriptedMetricTest {

    @Test
    public void should_parse_terms() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/scriptedmetric/scriptedmetric.json");

        ScriptedMetric scriptedMetric = ScriptedMetric.parse(XContentHelper.createParser(new BytesArray(json)), "profit");

        assertThat(scriptedMetric.getName()).isEqualTo("profit");
        assertThat(scriptedMetric.getValue()).isEqualTo("170");
    }
}