[![Build Status](https://travis-ci.org/OA-PASS/pass-indexer.png?branch=master)](https://travis-ci.org/OA-PASS/pass-indexer)


# Introduction

The pass-indexer keeps an [Elasticsearch](https://github.com/elastic/elasticsearch) index up to date with resources in a
[Fedora repository](https://fedorarepository.org/).

# Design

The pass-indexer monitors a JMS queue for messages about creation, deletion, and modification of Fedora resources.
(Fedora must be configured appropriately to setup this queue.)

The Elasticsearch index is created on startup if it does not exist with a set [configuration](pass-indexer-core/src/main/resources/esindex.json).
(That configuration default can be changed.)
If the index does exist, the configuration is retrieved from the index. In either case the mapping must match the documents which will be indexed.
The Elasticsearch document is the compact JSON-LD representation of that resource without server triples. If a key on the document is not present
in the mapping for the index, then the key is removed from the document and a warning is logged.

When there is a message about a resource of a type being monitored, the indexer either creates a corresponding document in Elasticsearch 
from the Fedora resource, updates such a document, or deletes the document.  Only messages about a resource of a type which matches a
configured prefix, PI_TYPE_PREFIX, are handled. The id of the Elasticsearch document is the safe URL base64 encoding of resource path. This lets both the document be created and updated with the same PUT.

# Auto-completion (suggestion) support

Suggestion is supported on field of type complection. In order to allow auto-completion on the values of a field
NAME, add a mapping for NAME_suggest of type completion. The field NAME_suggest will be automatically populated with the
contents of NAME by the indexer. The content of NAME will have a completion for the substring starting at each token. So
if the content is "token1 token2 token3", there will be completions, "token1 token2 token3", "token2 token3", and "token3".

A consquence of the auto-completion is that NAME_suggest fields will appear in the _source document returned by Elasticsearch.
See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-source-filtering.html for options.

You can search like below to find awardNumbers beginning with "R".

```
curl -X POST "http://localhost:9200/pass/_search?pretty" -H 'Content-Type: application/json' -d'
{
    "suggest": {
        "my-suggest" : {
            "prefix" : "R", 
            "completion" : { 
                "field" : "awardNumber_suggest" 
            }
        }
    }
}
'
```

In addition the projectName_suggest field is configured to have a category named pi with values of the pi field. Due to how Fedora URIs are handled, that value will be the resource path of the pi in Fedora.

Example doing a completion and restricting to a pi `http://localhost:8080/fcrepo/rest/users/ba/ea/af/8c/baeaaf8c-dfec-4681-a527-1711a453a2ed`. Generally the resource path can be produced from a Fedora uri by stripping off the prefix ending in `/rest`.


```
 curl 'http://localhost:9200/pass/_search?pretty' -H 'Content-Type: application/json; charset=UTF-8' --data-binary '
 {
    "suggest": {
        "projectName": {
            "prefix": "cre",
            "completion": {
                "field": "projectName_suggest",
                "context": {"pi": "/users/ba/ea/af/8c/baeaaf8c-dfec-4681-a527-1711a453a2ed"}
            }
        }
    }
}'
```

# Handling Fedora URIs

In PASS, a Fedora resource can be addressed in two different ways, by a public URI or a private URI. The public URI must pass through Shibboleth and can be used by the public
at large. The private URI is used by the backend services and allows them to avoid Shibboleth.

A custom normalizer is defined to handle fields containing fedora URIs mapping them to their Fedora resource path. This allows searches to be done using either the public
or private URI.

# Command line tool

The command line tool runs the indexer with the configuration either specifies as environment variable or system
properties. 

Configuration properties:
* PI_FEDORA_USER=admin
* PI_FEDORA_PASS=admin
* PI_ES_INDEX=http://elasticsearch:9200/pass/
* PI_ES_CONFIG=/file/or/resource/path/config.json
* PI_FEDORA_JMS_BROKER=tcp://fcrepo:61616
* PI_FEDORA_JMS_QUEUE=fedora
* PI_TYPE_PREFIX=http://example.org/pass/
* PI_LOG_LEVEL=debug


The PI_FEDORA_USER and PI_FEDORA_PASS are the credentials used to connect to Fedora with basic auth.

The PI_ES_INDEX is the index where Fedora documents are sent. If PI_ES_CONFIG is set to a file or
a classpath resource, it will be used as the Elasticsearch index configuration if the index does not
exist and is created.


