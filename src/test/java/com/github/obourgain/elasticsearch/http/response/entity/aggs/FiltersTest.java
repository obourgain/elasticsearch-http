package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class FiltersTest {

    @Test
    public void should_parse_filters() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/filters/filters.json");

        Filters filters = Filters.parse(XContentHelper.createParser(new BytesArray(json)), "foo");

        assertThat(filters.getName()).isEqualTo("foo");

        List<Filters.Bucket> buckets = filters.getBuckets();
        buckets.sort(new Comparator<Filters.Bucket>() {
            @Override
            public int compare(Filters.Bucket o1, Filters.Bucket o2) {
                return Long.compare(o1.getDocCount(), o2.getDocCount());
            }
        });

        Filters.Bucket bucket = buckets.get(0);
        assertThat(bucket.getDocCount()).isEqualTo(34);
        assertThat(bucket.getKey()).isEqualTo("the_first_filter");
        assertThat(bucket.getAggregations()).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo")).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo").getBuckets()).hasSize(1);

        bucket = buckets.get(1);
        assertThat(bucket.getDocCount()).isEqualTo(439);
        assertThat(bucket.getKey()).isEqualTo("the_second_filter");
        assertThat(bucket.getAggregations()).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo")).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo").getBuckets()).hasSize(1);
    }

    @Test
    public void should_parse_anonymous_filters() throws IOException {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/filters/anonymous_filters.json");

        Filters filters = Filters.parse(XContentHelper.createParser(new BytesArray(json)), "foo");

        assertThat(filters.getName()).isEqualTo("foo");

        List<Filters.Bucket> buckets = filters.getBuckets();
        buckets.sort(new Comparator<Filters.Bucket>() {
            @Override
            public int compare(Filters.Bucket o1, Filters.Bucket o2) {
                return Long.compare(o1.getDocCount(), o2.getDocCount());
            }
        });

        Filters.Bucket bucket = buckets.get(0);
        assertThat(bucket.getDocCount()).isEqualTo(34);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getAggregations()).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo")).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo").getBuckets()).hasSize(1);

        bucket = buckets.get(1);
        assertThat(bucket.getDocCount()).isEqualTo(439);
        assertThat(bucket.getKey()).isNull();
        assertThat(bucket.getAggregations()).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo")).isNotNull();
        assertThat(bucket.getAggregations().getTerms("foo").getBuckets()).hasSize(1);
    }
}