package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class DateRangeTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/daterange/daterange.json");

        DateRange ranges = new DateRange().parse(XContentHelper.createParser(new BytesArray(json)), "range");

        assertThat(ranges.getName()).isEqualTo("range");

        List<DateRange.Bucket> buckets = ranges.getBuckets();
        assertThat(buckets).hasSize(2);

        DateRange.Bucket bucket = buckets.get(0);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isNull();
        assertThat(bucket.getFromAsString()).isNull();
        assertThat(bucket.getTo()).isEqualTo(1.3437792E+12);
        assertThat(bucket.getToAsString()).isEqualTo("08-2012");
        assertThat(bucket.getDocCount()).isEqualTo(7);
        assertThat(bucket.getAggregations()).isNull();

        bucket = buckets.get(1);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getFrom()).isEqualTo(1.3437792E+12);
        assertThat(bucket.getFromAsString()).isEqualTo("08-2012");
        assertThat(bucket.getTo()).isNull();
        assertThat(bucket.getToAsString()).isNull();
        assertThat(bucket.getDocCount()).isEqualTo(2);
        assertThat(bucket.getAggregations()).isNull();
    }

}