package com.github.obourgain.elasticsearch.http.handler.document.bulk;

import static com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkItemTest.assertCreate;
import static com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkItemTest.assertDelete;
import static com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkItemTest.assertIndex;
import static com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkItemTest.assertUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class BulkResponseTest {

    @Test
    public void should_parse_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/document/bulk/bulk_response.json");

        BulkResponse bulkResponse = new BulkResponse().doParse(new BytesArray(json));

        assertThat(bulkResponse.getTook()).isGreaterThan(0);
        assertThat(bulkResponse.isErrors()).isTrue();

        List<BulkItem> items = bulkResponse.getItems();
        assertThat(items).isNotNull();
        assertThat(items).hasSize(4);

        assertIndex(items.get(0));
        assertDelete(items.get(1));
        assertCreate(items.get(2));
        assertUpdate(items.get(3));
    }
}