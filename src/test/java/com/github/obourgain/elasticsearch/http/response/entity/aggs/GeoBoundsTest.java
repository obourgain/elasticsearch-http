package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class GeoBoundsTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/aggs/geobounds/geobounds.json");

        GeoBounds geoBounds = GeoBounds.parse(XContentHelper.createParser(new BytesArray(json)), "geoBounds");

        assertThat(geoBounds.getTopLeftLat()).isEqualTo(45.04987137025941d);
        assertThat(geoBounds.getTopLeftLon()).isEqualTo(-162.1274162436077d);
        assertThat(geoBounds.getBottomRightLat()).isEqualTo(-85.86558887693678d);
        assertThat(geoBounds.getBottomRightLon()).isEqualTo(177.97394450132703d);
    }
}