package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class PercentilesTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/percentiles/percentiles.json");

        Percentiles percentilesAgg = Percentiles.parse(XContentHelper.createParser(new BytesArray(json)), "load_time_outlier");

        assertThat(percentilesAgg.getName()).isEqualTo("load_time_outlier");

        List<Percentile> percentiles = percentilesAgg.getPercentiles();
        assertThat(percentiles).hasSize(7);

        Percentile percentile = percentiles.get(0);
        assertThat(percentile.getKey()).isEqualTo(1.0d);
        assertThat(percentile.getValue()).isEqualTo(15);

        percentile = percentiles.get(1);
        assertThat(percentile.getKey()).isEqualTo(5.0d);
        assertThat(percentile.getValue()).isEqualTo(20);

        percentile = percentiles.get(2);
        assertThat(percentile.getKey()).isEqualTo(25.0d);
        assertThat(percentile.getValue()).isEqualTo(23);
    }

    @Test
    public void should_parse_keyed() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/percentiles/percentiles_without_key.json");

        Percentiles percentilesAgg = Percentiles.parse(XContentHelper.createParser(new BytesArray(json)), "load_time_outlier");

        assertThat(percentilesAgg.getName()).isEqualTo("load_time_outlier");

        List<Percentile> percentiles = percentilesAgg.getPercentiles();
        assertThat(percentiles).hasSize(7);

        Percentile percentile = percentiles.get(0);
        assertThat(percentile.getKey()).isEqualTo(1);
        assertThat(percentile.getValue()).isEqualTo(2.1d);

        percentile = percentiles.get(1);
        assertThat(percentile.getKey()).isEqualTo(5);
        assertThat(percentile.getValue()).isEqualTo(2.5d);

        percentile = percentiles.get(2);
        assertThat(percentile.getKey()).isEqualTo(25);
        assertThat(percentile.getValue()).isEqualTo(4.5d);
    }
}