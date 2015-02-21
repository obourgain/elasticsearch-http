package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class HistogramTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/histogram/histogram.json");

        Histogram prices = Histogram.parse(XContentHelper.createParser(new BytesArray(json)), "price");

        assertThat(prices.getName()).isEqualTo("price");

        List<Histogram.Bucket> buckets = prices.getBuckets();
        assertThat(buckets).hasSize(3);

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo(0);
        assertThat(bucket.getKeyAsString()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(2);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo(50);
        assertThat(bucket.getKeyAsString()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(2);
        assertThat(bucket.getKey()).isEqualTo(150);
        assertThat(bucket.getKeyAsString()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(3);
        assertThat(bucket.getAggregations()).isNull();
    }

    @Test
    public void should_parse_keyed() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/histogram/histogram_keyed.json");

        Histogram prices = Histogram.parse(XContentHelper.createParser(new BytesArray(json)), "price");

        assertThat(prices.getName()).isEqualTo("price");

        List<Histogram.Bucket> buckets = prices.getBuckets();
        assertThat(buckets).hasSize(3);

        Histogram.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo(0);
        assertThat(bucket.getKeyAsString()).isEqualTo("0");
        assertThat(bucket.getDocCount()).isEqualTo(2);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo(50);
        assertThat(bucket.getKeyAsString()).isEqualTo("50");
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(2);
        assertThat(bucket.getKey()).isEqualTo(150);
        assertThat(bucket.getKeyAsString()).isEqualTo("150");
        assertThat(bucket.getDocCount()).isEqualTo(3);
        assertThat(bucket.getAggregations()).isNull();
    }

}