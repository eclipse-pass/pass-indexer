package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class FedoraIndexerServiceTest {
    private MockWebServer server;
    private HttpUrl es_index_url;
    private FedoraIndexerService service;
    private String queue;
    
    
    @Before
    public void setup() throws Exception {
        EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

        server = new MockWebServer();
        
        // GET for Elasticsearch index config
        server.enqueue(new MockResponse().setResponseCode(404));
        
        // PUT for Elasticsearch index config
        server.enqueue(new MockResponse().setBody("{}"));
        
        es_index_url = server.url("/es/test/");
        queue = "fedora";
        service = new FedoraIndexerService();
        service.setAllowedTypePrefix("http://oapass.org/");
        service.setElasticsearchIndexUrl(es_index_url.toString());
        service.setElasticsearchIndexConfig("/esindex.json");
        service.setFedoraUser("moo");
        service.setFedoraPass("moo");
        service.setJmsConnectionFactory(broker.createConnectionFactory());
        service.setJmsQueue(queue);
        
        service.start();
        
        // Drain index config requests
        server.takeRequest();
        server.takeRequest();
    }
    
    @After
    public void cleanup() throws IOException {
        server.shutdown();
        service.close();
    }

    // Test handling of a create message added to the JMS queue.
    @Test
    public void testCreationMessage() throws Exception {
        JmsClient jms_client = service.getJmsClient();

        // Fedora resource created
        String fedora_res_uri = server.url("/fcrepo/grant/4").toString();
        
        JSONObject res_json = new JSONObject();
        res_json.put("@id", fedora_res_uri);
        res_json.put("@context",
                "https://raw.githubusercontent.com/OA-PASS/ember-fedora-adapter/master/tests/dummy/public/farm.jsonld");
        res_json.put("healthy", true);
        res_json.put("name", "moo");

        // GET for Fedora resource
        server.enqueue(new MockResponse().setBody(res_json.toString()));

        // POST to Elasticsearch
        server.enqueue(new MockResponse().setBody("{}"));
        
        // Message indicating resource has been created
        String msg = "{\n" + 
                "    \"id\": \"" + fedora_res_uri + "\",\n" + 
                "    \"type\": [\n" + 
                "        \"http://www.w3.org/ns/ldp#Container\",\n" + 
                "        \"http://oapass.org/ns/pass#Grant\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Resource\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Container\",\n" + 
                "        \"http://www.w3.org/ns/ldp#RDFSource\",\n" + 
                "        \"http://www.w3.org/ns/prov#Entity\"\n" + 
                "    ],\n" + 
                "    \"isPartOf\": \"http://fcrepo:8080/fcrepo/rest\",\n" + 
                "    \"wasGeneratedBy\": {\n" + 
                "        \"type\": [\n" + 
                "            \"http://fedora.info/definitions/v4/event#ResourceModification\",\n" + 
                "            \"http://fedora.info/definitions/v4/event#ResourceCreation\",\n" + 
                "            \"http://www.w3.org/ns/prov#Activity\"\n" + 
                "        ],\n" + 
                "        \"identifier\": \"urn:uuid:c87039ad-9b83-43d7-94fa-ddc474b780ef\",\n" + 
                "        \"atTime\": \"2018-04-10T13:37:49.667Z\"\n" + 
                "    },\n" + 
                "    \"wasAttributedTo\": [\n" + 
                "        {\n" + 
                "            \"type\": \"http://www.w3.org/ns/prov#Person\",\n" + 
                "            \"name\": \"admin\"\n" + 
                "        },\n" + 
                "        {\n" + 
                "            \"type\": \"http://www.w3.org/ns/prov#SoftwareAgent\",\n" + 
                "            \"name\": \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36\"\n" + 
                "        }\n" + 
                "    ]\n}";
        
        jms_client.write(queue, jms_client.getSessionSupplier().get().createTextMessage(msg));
        
        // Wait for message to be handled.
        Thread.sleep(5 * 1000);
        
        // Check the requests

        RecordedRequest get = server.takeRequest();

        assertEquals("GET", get.getMethod());
        assertNotNull(get.getHeader("Authorization"));
        assertEquals("application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"", get.getHeader("Accept"));
        assertEquals(fedora_res_uri, get.getRequestUrl().toString());

        RecordedRequest post = server.takeRequest();

        assertEquals("POST", post.getMethod());
        
        JSONObject payload = new JSONObject(post.getBody().readUtf8());
        
        assertEquals(res_json.get("@id"), payload.get("@id"));
        assertEquals("application/json; charset=utf-8", post.getHeader("Content-Type"));
        assertTrue(post.getRequestUrl().toString().startsWith(es_index_url.toString()));
    }
}
