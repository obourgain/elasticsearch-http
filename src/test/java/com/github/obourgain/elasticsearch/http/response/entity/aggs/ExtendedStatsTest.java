package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class ExtendedStatsTest {
    @Test
    public void should_parse_bucket() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/stats/extended-stats.json");

        ExtendedStats stats = ExtendedStats.parse(XContentHelper.createParser(new BytesArray(json)), "grades_stats");

        assertThat(stats.getCount()).isEqualTo(9);
        assertThat(stats.getMin()).isEqualTo(72);
        assertThat(stats.getMax()).isEqualTo(99);
        assertThat(stats.getAvg()).isEqualTo(86);
        assertThat(stats.getSum()).isEqualTo(774);
        assertThat(stats.getSumOfSqrs()).isEqualTo(67028);
        assertThat(stats.getVariance()).isEqualTo(51.55555555555556d);
        assertThat(stats.getStdDev()).isEqualTo(7.180219742846005d);
        assertThat(stats.getStdDevBounds().getUpper()).isEqualTo(100.36043948569201d);
        assertThat(stats.getStdDevBounds().getLower()).isEqualTo(71.63956051430799d);
    }
}