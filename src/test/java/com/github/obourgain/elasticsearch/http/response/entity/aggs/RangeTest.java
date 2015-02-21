package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class RangeTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/range/range.json");

        Range ranges = Range.parse(XContentHelper.createParser(new BytesArray(json)), "price_ranges");

        assertThat(ranges.getName()).isEqualTo("price_ranges");

        List<Range.Bucket> buckets = ranges.getBuckets();
        assertThat(buckets).hasSize(3);

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isNull();
        assertThat(bucket.getTo()).isEqualTo(50);
        assertThat(bucket.getDocCount()).isEqualTo(2);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isEqualTo(50);
        assertThat(bucket.getTo()).isEqualTo(100);
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(2);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isEqualTo(100);
        assertThat(bucket.getTo()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();
    }

    @Test
    public void should_parse_keyed() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/range/range_keyed.json");

        Range ranges = Range.parse(XContentHelper.createParser(new BytesArray(json)), "price_ranges");

        assertThat(ranges.getName()).isEqualTo("price_ranges");

        List<Range.Bucket> buckets = ranges.getBuckets();
        assertThat(buckets).hasSize(3);

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo("*-50.0");
        assertThat(bucket.getFrom()).isNull();
        assertThat(bucket.getTo()).isEqualTo(50);
        assertThat(bucket.getDocCount()).isEqualTo(2);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo("50.0-100.0");
        assertThat(bucket.getFrom()).isEqualTo(50);
        assertThat(bucket.getTo()).isEqualTo(100);
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(2);
        assertThat(bucket.getKey()).isEqualTo("100.0-*");
        assertThat(bucket.getFrom()).isEqualTo(100);
        assertThat(bucket.getTo()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();
    }

}