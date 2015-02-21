package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class GlobalTest {

    @Test
    public void should_parse_global() throws IOException {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/global/global.json");
        Global global = new Global().parse(XContentHelper.createParser(new BytesArray(json)), "all_products");

        assertThat(global.getDocCount()).isEqualTo(100);
        assertThat(global.getAggregations()).isNotNull();
        assertThat(global.getAggregations().getAvg("avg_price")).isNotNull();
        assertThat(global.getAggregations().getAvg("avg_price").getValue()).isEqualTo(56.3d);
    }

    @Test
    public void should_parse_global_with_several_sub_aggs() throws IOException {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/global/global_with_several_sub_aggs.json");
        Global global = new Global().parse(XContentHelper.createParser(new BytesArray(json)), "all_products");

        assertThat(global.getDocCount()).isEqualTo(100);
        assertThat(global.getAggregations().getAvg("avg_price")).isNotNull();
        assertThat(global.getAggregations().getAvg("avg_price").getValue()).isEqualTo(56.3d);

        assertThat(global.getAggregations().getMax("max_price")).isNotNull();
        assertThat(global.getAggregations().getMax("max_price").getValue()).isEqualTo(78);
    }
}