package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class PercentileRanksTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/percentileranks/percentileranks.json");

        PercentileRanks percentilesAgg = new PercentileRanks().parse(XContentHelper.createParser(new BytesArray(json)), "ranks");

        assertThat(percentilesAgg.getName()).isEqualTo("ranks");

        List<Percentile> percentiles = percentilesAgg.getPercentiles();
        assertThat(percentiles).hasSize(5);

        Percentile percentile = percentiles.get(0);
        assertThat(percentile.getKey()).isEqualTo(0);
        assertThat(percentile.getValue()).isEqualTo(0);

        percentile = percentiles.get(1);
        assertThat(percentile.getKey()).isEqualTo(5);
        assertThat(percentile.getValue()).isEqualTo(12);

        percentile = percentiles.get(2);
        assertThat(percentile.getKey()).isEqualTo(15);
        assertThat(percentile.getValue()).isEqualTo(40);

        percentile = percentiles.get(3);
        assertThat(percentile.getKey()).isEqualTo(16);
        assertThat(percentile.getValue()).isEqualTo(45);

        percentile = percentiles.get(4);
        assertThat(percentile.getKey()).isEqualTo(17);
        assertThat(percentile.getValue()).isEqualTo(100);
    }

    @Test
    public void should_parse_keyed() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/percentileranks/percentileranks_without_key.json");

        PercentileRanks percentilesAgg = new PercentileRanks().parse(XContentHelper.createParser(new BytesArray(json)), "ranks");

        assertThat(percentilesAgg.getName()).isEqualTo("ranks");

        List<Percentile> percentiles = percentilesAgg.getPercentiles();
        assertThat(percentiles).hasSize(5);


        Percentile percentile = percentiles.get(0);
        assertThat(percentile.getKey()).isEqualTo(0);
        assertThat(percentile.getValue()).isEqualTo(0);

        percentile = percentiles.get(1);
        assertThat(percentile.getKey()).isEqualTo(5);
        assertThat(percentile.getValue()).isEqualTo(12);

        percentile = percentiles.get(2);
        assertThat(percentile.getKey()).isEqualTo(15);
        assertThat(percentile.getValue()).isEqualTo(40);

        percentile = percentiles.get(3);
        assertThat(percentile.getKey()).isEqualTo(16);
        assertThat(percentile.getValue()).isEqualTo(45);

        percentile = percentiles.get(4);
        assertThat(percentile.getKey()).isEqualTo(17);
        assertThat(percentile.getValue()).isEqualTo(100);
    }
}