package org.dataconservancy.pass.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


/**
 * Maintain documents corresponding to the JSON-LD representation of Fedora resources.
 * The compact JSON-LD representation must be simple enough for Elasticsearch to understand.
 * Documents are updated when Fedora resources are created or modified or deleted.
 * The document id is the path of the Fedora URI encoded as base64 safe for URLs.
 * 
 * The mapping in the index configuration is used to check JSON documents retrieved from Fedora 
 * before indexing. Properties which do not have a mapping or otherwise cannot be indexed are
 * logged and ignored. On start the index is checked.
 * 
 * Properties which contain Fedora URIs have a custom matching with causes them to be indexed as
 * Fedora resource paths. This allows a client to search using different URIs which map to the same
 * Fedora resource.
 */
public class ElasticSearchIndexer implements IndexerConstants {
    private static final String SUGGEST_SUFFIX = "_suggest";
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIndexer.class);

    private final OkHttpClient client;
    private final String fedora_cred;
    private final String es_index_url;
    private final Set<String> supported_fields;
    
    // Fields which have a _suggest companion field of type completion.
    private final Set<String> suggest_fields; 
    
    
    /**
     * If the given Elasticsearch index does not exist, create it using the supplied configuration.
     * Otherwise the configuration is retrieved from the index.
     * 
     * @param es_index_url
     * @param es_index_config - Either file or resource path to index config. If null, use provided PASS configuration.
     * @param fedora_user
     * @param fedora_pass
     * @throws IOException
     */
    public ElasticSearchIndexer(String es_index_url, String es_index_config, String fedora_user, String fedora_pass) throws IOException {
        this.client = new OkHttpClient.Builder().connectTimeout(60, TimeUnit.SECONDS).writeTimeout(60, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS).build();
        this.es_index_url = es_index_url.endsWith("/") ? es_index_url : es_index_url + "/";
        this.fedora_cred = Credentials.basic(fedora_user, fedora_pass);
        
        JSONObject config = get_existing_index_configuration();

        if (config == null) {
            LOG.info("Index does not exist. Creating " + es_index_url + " with config " + es_index_config);
            config = load_index_configuration(es_index_config);
            create_index(config);
        } else {
            LOG.info("Found existing index " + es_index_url);
        }
        
        // Determine the available fields in the index from the configuration and which fields support completion.
        
        // The mappings key is either toplevel or inside an object representing the index.
        if (!config.has("mappings")) {
            Set<String> keys = config.keySet();
            
            if (keys.size() == 1) {
                config = config.getJSONObject(keys.iterator().next());
            }
        }
        
        JSONObject props = config.getJSONObject("mappings").getJSONObject("_doc").getJSONObject("properties");
        this.supported_fields = new HashSet<>(props.keySet());
        this.suggest_fields = supported_fields.stream().filter(f -> f.endsWith(SUGGEST_SUFFIX)).map(f -> 
                f.substring(0, f.length() - SUGGEST_SUFFIX.length())).collect(Collectors.toSet());
    }
   
    // Create index es_index_url with the given configuration
    private void create_index(JSONObject config) throws IOException {
        RequestBody body = RequestBody.create(JSON, config.toString());
        Request put = new Request.Builder().url(es_index_url).put(body).build();
        
        try (Response response = client.newCall(put).execute()) {
            String result = response.body().string();

            if (response.isSuccessful()) {
                LOG.info("Created index: " + es_index_url + "\n" + result);
            } else {
                String msg = "Failed to create index: " + es_index_url + "\n" + result;
                LOG.error(msg);
                throw new IOException(msg);
            }
        }
    }
    
    // Return the index configuration specified as a file, resource, or URL.
    private JSONObject load_index_configuration(String es_index_config) throws IOException {
        boolean is_url = false;

        try {
            new URL(es_index_config);
            is_url = true;
        } catch (MalformedURLException e) {
        }

        if (is_url) {
            LOG.info("Loading index configuration from URL: " + es_index_config);

            Request get = new Request.Builder().url(es_index_config).build();

            try (Response response = client.newCall(get).execute()) {
                if (!response.isSuccessful()) {
                    String msg = "Failed to retrieve specified index config: " + es_index_config + " "
                            + response.code();
                    LOG.error(msg);
                    throw new IOException(msg);
                }

                return new JSONObject(response.body().string());
            }
        }
 
        Path path = Paths.get(es_index_config);

        if (Files.exists(path)) {
            LOG.info("Loading index configuration from file: " + es_index_config);

            try (InputStream is = Files.newInputStream(path)) {
                return new JSONObject(new JSONTokener(is));
            }
        }

        LOG.info("Loading index configuration from classpath: " + es_index_config);

        try (InputStream is = this.getClass().getResourceAsStream(es_index_config)) {
            if (is == null) {
                String msg = "Index configuration not found on classpath: " + es_index_config;
                LOG.error(msg);
                throw new IOException(msg);
            }

            return new JSONObject(new JSONTokener(is));
        }
    }

    // Return the current index configuration or null if it does not exist.
    private JSONObject get_existing_index_configuration() throws IOException {
        Request get = new Request.Builder().url(es_index_url).build();
        
        try (Response response = client.newCall(get).execute()) {
            if (response.code() == 404) {
                return null;
            }
        
            if (!response.isSuccessful()) {
                String msg = "Failed to retrieve index config: " + es_index_url + " " + response.code();
                LOG.error(msg);
                throw new IOException(msg);
            }

            return new JSONObject(response.body().string());
        }
    }
    
    // Return compact JSON-LD representation of Fedora resource without server triples
    // Return null if resource is now a tombstone.
    private String get_fedora_resource(String uri) throws IOException {
        Request get = new Request.Builder().url(uri).header("Authorization", fedora_cred)
                .header("Accept", FEDORA_ACCEPT_HEADER).header("Prefer", FEDORA_PREFER_HEADER).build();

        try (Response response = client.newCall(get).execute()) {
            if (!response.isSuccessful()) {
                if (response.code() == 410) {
                    return null;
                }
                
                String msg = "Failed to retrieve Fedora resource: " + uri + " " + response.code();
                LOG.error(msg);
                throw new IOException(msg);
            }

            String doc = response.body().string();
            if (!response.header("content-type", "application/ld+json").contains("json")) {
                return null;
            }
            return doc;
        }
    }
    
    // Return URL safe base64 encoding of string.
    private String base64_encode(String s) {
        return Base64.getUrlEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    // Return URL safe document id.
    private String get_document_id(String fedora_uri) throws IOException {
        return base64_encode(new URL(fedora_uri).getPath());
    }

    private String get_create_document_url(String doc_id) throws IOException {
        return es_index_url + "_doc/" + doc_id + "?pretty";
    }

    // Do any normalization necessary before indexing.
    // Ignore and warn about keys not in the configuration or with object values
    
    private String normalize_document(String json) {
        JSONObject o = new JSONObject(json);
        
        for (String key: JSONObject.getNames(o)) {
            Object value = o.get(key);

            if (!supported_fields.contains(key)) {
                LOG.warn("Unexpected property ignored: " + key + ", " + value);
                o.remove(key);
            } else if (JSONObject.class.isInstance(value)) {
                LOG.warn("Property with object value ignored: " + key + ", " + value);
                o.remove(key);
            } else if (suggest_fields.contains(key)) {
                o.put(key + SUGGEST_SUFFIX, construct_completions(value.toString(), o));
            }
        }
        
        return o.toString();
    }

    // Text with n tokens separated by whitespace is turned into n completions,
    // one for each token. Each completion starts at the token and finishes at
    // the end of the text.
    private JSONArray construct_completions(String text, JSONObject o) {
        JSONArray result = new JSONArray();
        
        int completion = 0;
        boolean whitespace = true;
        
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            
            if (whitespace) {
                if (!Character.isWhitespace(c)) {
                    whitespace = false;
                    
                    result.put(completion++, text.substring(i));
                }
            } else  {
                if (Character.isWhitespace(c)) {
                    whitespace = true;
                }
            }
        }
                
        return result;
    }

    // Create or update the document corresponding to a Fedora resource in Elasticsearch.
    public String update_document(String fedora_uri) throws IOException {
        LOG.debug("Updating document for Fedora resource: " + fedora_uri);

        String fedora_json = get_fedora_resource(fedora_uri);

        if (fedora_json == null) {
            // Fedora resource was deleted. Assume a delete message is coming.
            LOG.debug("Fedora resource was deleted: " + fedora_uri);
            return null;
        }
        
        String doc = normalize_document(fedora_json);
        String doc_id = get_document_id(fedora_uri);
        String doc_url = get_create_document_url(doc_id);

        RequestBody body = RequestBody.create(JSON, doc);
        Request post = new Request.Builder().url(doc_url).post(body).build();
        
        try (Response response = client.newCall(post).execute()) {
            String result = response.body().string();

            if (response.isSuccessful()) {
                LOG.debug("Update success: " + response);
            } else {
                String msg = "Update failure: " + result; 
                LOG.error(msg);
                throw new IOException(msg);
            }
        }

        return doc;
    }

    private void delete_document(String fedora_uri) throws IOException {
        LOG.debug("Deleting document for Fedora resource: " + fedora_uri);

        String doc_id = get_document_id(fedora_uri);
        String doc_url = get_create_document_url(doc_id);

        Request delete = new Request.Builder().url(doc_url).delete().build();
        
        try (Response response = client.newCall(delete).execute()) {
            String result = response.body().string();

            if (response.isSuccessful()) {
                LOG.debug("Delete success: " + response);
            } else {
                LOG.warn("Delete failed: " + result);
            }
        }
    }

    public void handle(FedoraMessage m) throws IOException {
        LOG.debug("Handling Fedora message: " + m);

        switch (m.getAction()) {
        case CREATED:
        case MODIFIED:
            update_document(m.getResourceURI());
            break;
        case DELETED:
            delete_document(m.getResourceURI());
            break;
        default:
            break;
        }
    }
}
