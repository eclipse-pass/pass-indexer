package org.dataconservancy.pass.indexer;

import okhttp3.MediaType;

/**
 * Shared constants.
 */
public interface IndexerConstants {
    String FEDORA_ACCEPT_HEADER = "application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"";
    String FEDORA_PREFER_HEADER =
        "return=representation; omit=\"http://fedora.info/definitions/v4/repository#ServerManaged\"";
    String FEDORA_PREFER_LENIENT_HEADER = "handling=lenient; received=\"minimal\"";

    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
}
