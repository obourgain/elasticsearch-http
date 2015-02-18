package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class StatsTest {
    @Test
    public void should_parse_bucket() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/stats/stats.json");

        Stats stats = Stats.parse(XContentHelper.createParser(new BytesArray(json)), "grades_stats");

        assertThat(stats.getCount()).isEqualTo(6);
        assertThat(stats.getMin()).isEqualTo(60);
        assertThat(stats.getMax()).isEqualTo(98);
        assertThat(stats.getAvg()).isEqualTo(78.5d);
        assertThat(stats.getSum()).isEqualTo(471);
    }
}