elasticsearch-http
==================

An elasticsearch client library similar to the transport client but using HTTP.
The inputs of the client are similar to the transport client, so you can reuse your builders.
The object returned are different and based on the returned JSON.

This is not fully production ready nor production proof, some features are missing.

If you use it, even just to test, I would love to hear your feedback and do not hesitate to open issues or contribute.

The current version uses RxNetty under the hood, but that may change.
An API using Rx is planned but not yet developed, the current API exposes sync and async variant of each method like the transport client.

### Building
Clone the repository and run ```mvn package``` to build it and run tests or ```mvn install``` to make it available in your local maven repository.

### What works (or is supposed to) :
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

### What does not work or is not supported
These features are either not implemented or too work-in-progress to be used with the http client
* suggests : still work to do
* facets : Those will be removed in 2.0 so I will not bother to implement support for those
* admin apis : do NOT use it, some method are defined but not implemented or parsing responses may not be implemented.
* search shard
* search template
* _analyze : not implemented
* probably a lot of other stuff

### How to use it

Get the URL of at least one Elasticsearch node, with the _http_ port, and not the _transport_ port. 
By default the port is in the range 9200-9299.
  
Create an instance of _HttpClient_ and give it the URLs of your nodes.

```
HttpClient client = new HttpClient("localhost:9200");
```

Remember to ```close()``` when you are done.