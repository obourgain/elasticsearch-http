package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class SignificantTermsTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/significantterms/significantterms.json");

        SignificantTerms significantTerms = SignificantTerms.parse(XContentHelper.createParser(new BytesArray(json)), "significantCrimeTypes");

        assertThat(significantTerms.getName()).isEqualTo("significantCrimeTypes");
        Assertions.assertThat(significantTerms.getDocCount()).isEqualTo(47347);

        List<SignificantTerms.Bucket> buckets = significantTerms.getBuckets();
        assertThat(buckets).hasSize(1);

        SignificantTerms.Bucket bucket = buckets.get(0);
        Assertions.assertThat(bucket.getKey()).isEqualTo("Bicycle theft");
        Assertions.assertThat(bucket.getDocCount()).isEqualTo(3640);
        Assertions.assertThat(bucket.getScore()).isEqualTo(0.371235374214817d);
        Assertions.assertThat(bucket.getBgCount()).isEqualTo(66799);
        Assertions.assertThat(bucket.getAggregations()).isNull();
    }

}