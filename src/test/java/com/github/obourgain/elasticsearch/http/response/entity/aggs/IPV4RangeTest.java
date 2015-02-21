package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class IPV4RangeTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/ipv4range/ipv4range.json");

        IPV4Range ranges = IPV4Range.parse(XContentHelper.createParser(new BytesArray(json)), "ip_ranges");

        assertThat(ranges.getName()).isEqualTo("ip_ranges");

        List<IPV4Range.Bucket> buckets = ranges.getBuckets();
        assertThat(buckets).hasSize(2);

        IPV4Range.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isNull();
        assertThat(bucket.getFromAsString()).isNull();
        assertThat(bucket.getTo()).isEqualTo(167772165);
        assertThat(bucket.getToAsString()).isEqualTo("10.0.0.5");
        assertThat(bucket.getDocCount()).isEqualTo(4);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isEqualTo(167772165);
        assertThat(bucket.getFromAsString()).isEqualTo("10.0.0.5");
        assertThat(bucket.getTo()).isNull();
        assertThat(bucket.getToAsString()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(6);
        assertThat(bucket.getAggregations()).isNull();
    }

    @Test
    public void should_parse_with_mask() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/ipv4range/ipv4range_with_mask.json");

        IPV4Range ranges = IPV4Range.parse(XContentHelper.createParser(new BytesArray(json)), "ip_ranges");

        assertThat(ranges.getName()).isEqualTo("ip_ranges");

        List<IPV4Range.Bucket> buckets = ranges.getBuckets();
        assertThat(buckets).hasSize(2);

        IPV4Range.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isEqualTo("10.0.0.0/25");
        assertThat(bucket.getFrom()).isEqualTo(1.6777216E+8);
        assertThat(bucket.getFromAsString()).isEqualTo("10.0.0.0");
        assertThat(bucket.getTo()).isEqualTo(167772287);
        assertThat(bucket.getToAsString()).isEqualTo("10.0.0.127");
        assertThat(bucket.getDocCount()).isEqualTo(127);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isEqualTo("10.0.0.127/25");
        assertThat(bucket.getFrom()).isEqualTo(1.6777216E+8);
        assertThat(bucket.getFromAsString()).isEqualTo("10.0.0.0");
        assertThat(bucket.getTo()).isEqualTo(167772287);
        assertThat(bucket.getToAsString()).isEqualTo("10.0.0.127");
        assertThat(bucket.getDocCount()).isEqualTo(127);
        assertThat(bucket.getAggregations()).isNull();
    }

}