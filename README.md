elasticsearch-http
==================

[![Build Status](https://travis-ci.org/obourgain/elasticsearch-http.svg)](https://travis-ci.org/obourgain/elasticsearch-http)

An elasticsearch client library similar to the transport client but using HTTP.
The inputs of the client are similar to the transport client, so you can reuse your builders.
The object returned are different and based on the returned JSON.

This is not fully production-ready nor production proof, some features are missing.

If you use it, even just to test, I would love to hear your feedback and do not hesitate to open issues or contribute.

The current version uses RxNetty under the hood, but that may change.
An API using Rx is planned but not yet developed, the current API exposes sync and async variant of each method like the transport client.

### Compatibility
<table>
    <tr>
        <th>Elasticsearch version</th>
    </tr>
    <tr>
        <td>1.4.x</td>
    </tr>
    <tr>
        <td>1.5.x</td>
    </tr>
    <tr>
        <td>1.6.x</td>
    </tr>
</table> 

### Building
Clone the repository and run ```mvn package``` to build it and run tests or ```mvn install``` to make it available in your local maven repository.

### Features :
* search with queries/filters
* aggregations
* document APIs (get, insert, update, delete ...)
* bulks
* multiget
* delete by query
* termvectors & multi termvectors
* scroll
* explain
* percolate & multipercolate

### Not supported
These features are either not implemented or too work-in-progress to be used with the http client
* suggests : still work to do
* facets : (already deprecated)
* admin apis : work in progress
* search shard
* search template
* _analyze : not implemented
* probably a lot of other stuff

### Getting started

Get the URL of at least one Elasticsearch node, with the _http_ port, and not the _transport_ port. 
By default the port is in the range 9200-9299.
  
Create an instance of _HttpClient_ and give it the URLs of your nodes.

```
HttpClient client = new HttpClient("localhost:9200");
SearchRequest searchRequest = new SearchRequest("the_index").types("the_type").source(new SearchSourceBuilder().query(matchAllQuery()));
SearchResponse searchResponse = httpClient.search(searchRequest).get();
Hits hits = searchResponse.getHits();
```

Remember to ```close()``` when you are done.
