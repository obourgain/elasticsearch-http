elasticsearch-http
==================

POC of an elasticsearch client library implementing the same Java API as the classic Java client but over the HTTP/Json API protocol.

This is not fully production ready nor production proof, lots of features are missing as well as some response object can not be retaured from the JSON due to missing data.

If you use it, even just to test, I would love to hear your feedback and do not hesitate to open issues.

### Tests
This project depends on https://github.com/obourgain/reflection-matcher for tests to compare the http client's returned object grath with the one from the transport client.

There are also more test not included in this repository as the http client is tested against Elasticsearch's test suite by replacing the TransportClient by the HttpClient.
A good part of the tests are OK, but there is still a lot of work to do.
The code to launch Elasticsearch's test suite is really hack-ish for now and not yet published.

### What works :
* queries/filters (all types, this is not dependant on the client used)
* index
* bulks
* delete

### What work partially
* update : still some quirks to fix in regard to versioning
* get : I still have strange issues with get from the translog
* aggs : expect significant_terms. For terms variant, please use the more specific 'sterms', 'lterms' etc instead of the generic 'terms'. Maybe some other details may not work like reading the doc count error from the response in term aggs.
* Exception mapping : a good part of exception returned by Elasticsearch as Strings are mapped and thrown
* IndicesAdmin & ClusterAdmin : while most features are implemented, reading the response can be tricky. So I would not recommend you to use it to read cluster informations. Updating settings/mapping shouldbe fine but I focus mostly on the 'queries' part of the client for now

### What does not work
These features are either not implemented or too work-in-progress to be used with the http client
* suggests : WIP
* term vectors : Mapping seems to be a bit hardcore
* facets : Those will be removed in 2.0 so I will not bother to map them
* explain : not yet implemented
* serializing the response using Elasticsearch's binary format. Some interfaces are reimplemented where it was not practical to use Elasticsearch's class
* date histogram with custom date format
* multi search : not yet implemented
* multi percolate : not yet implemented
* template query : not yet implemented
* benchmark : not yet implemented
* more like this : does not pass ES test suite anymore
* source filtering in multi get
* get field in bulk update : bugs to be fixed
* scripts can not be indexed : not implemented
* explain : not implemented
* _analyze : not implemented

### What may differ from the TransportClient
The data types returned may change, in particular for numerics.
In particular, if you expect a `Long` and cast a value to `Long`, you may get a `ClassCastException` because the value was mapped to `Integer`.
To avoid this kind of issues, it is advised to cast to `Number` and use the `xxxValue()` methods. E.g. for the `Long`, you would write ((Number) <someMethod>()).longValue().
The same applies for arrays, the http client will return a `List`.
The order of fields in a `Map` may differ too as it is implementation-dependent.

Serializing the responses using Elasticsearch's binary format may not work because some classes are new implementations of interfaces.

Histogram aggs may have a different length as the TransportClient does not deduplicate ranges if the same range is asked several times.