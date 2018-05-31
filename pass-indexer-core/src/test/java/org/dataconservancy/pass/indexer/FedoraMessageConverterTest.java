package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class FedoraMessageConverterTest {

    private Set<String> to_set(String... array) {
        return new HashSet<String>(Arrays.asList(array));
    }

    @Test
    public void testConvertCreated() {
        String jms_text_msg = "{\n" + 
                "    \"id\": \"http://fcrepo:8080/fcrepo/rest/grants/30/46/79/7e/3046797e-227c-4da0-ae95-e1828561ca5f\",\n" + 
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
                "    ],\n" + 
                "    \"@context\": {\n" + 
                "        \"prov\": \"http://www.w3.org/ns/prov#\",\n" + 
                "        \"foaf\": \"http://xmlns.com/foaf/0.1/\",\n" + 
                "        \"dcterms\": \"http://purl.org/dc/terms/\",\n" + 
                "        \"xsd\": \"http://www.w3.org/2001/XMLSchema#\",\n" + 
                "        \"type\": \"@type\",\n" + 
                "        \"id\": \"@id\",\n" + 
                "        \"name\": {\n" + 
                "            \"@id\": \"foaf:name\",\n" + 
                "            \"@type\": \"xsd:string\"\n" + 
                "        },\n" + 
                "        \"identifier\": {\n" + 
                "            \"@id\": \"dcterms:identifier\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"isPartOf\": {\n" + 
                "            \"@id\": \"dcterms:isPartOf\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"atTime\": {\n" + 
                "            \"@id\": \"prov:atTime\",\n" + 
                "            \"@type\": \"xsd:dateTime\"\n" + 
                "        },\n" + 
                "        \"wasAttributedTo\": {\n" + 
                "            \"@id\": \"prov:wasAttributedTo\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"wasGeneratedBy\": {\n" + 
                "            \"@id\": \"prov:wasGeneratedBy\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        }\n" + 
                "    }\n" + 
                "}\n";
        
        FedoraMessage m = FedoraMessageConverter.convert(jms_text_msg);
        
        assertEquals(FedoraAction.CREATED, m.getAction());
        assertEquals("http://fcrepo:8080/fcrepo/rest/grants/30/46/79/7e/3046797e-227c-4da0-ae95-e1828561ca5f", m.getResourceURI());
        
        
        String[] expected_types = new String[] {
            "http://www.w3.org/ns/ldp#Container", 
            "http://oapass.org/ns/pass#Grant", 
            "http://fedora.info/definitions/v4/repository#Resource",
            "http://fedora.info/definitions/v4/repository#Container",
            "http://www.w3.org/ns/ldp#RDFSource",
            "http://www.w3.org/ns/prov#Entity"
        };
        
        assertEquals(to_set(expected_types), to_set(m.getResourceTypes()));
    }
    
    @Test
    public void testConvertModified() {
        String jms_text_msg = "{\n" + 
                "    \"id\": \"http://fcrepo:8080/fcrepo/rest/submissions/7f/cc/6f/c5/7fcc6fc5-10c8-476d-b537-cf13f20d9be7\",\n" + 
                "    \"type\": [\n" + 
                "        \"http://www.w3.org/ns/ldp#Container\",\n" + 
                "        \"http://oapass.org/ns/pass#Submission\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Resource\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Container\",\n" + 
                "        \"http://www.w3.org/ns/ldp#RDFSource\",\n" + 
                "        \"http://www.w3.org/ns/prov#Entity\"\n" + 
                "    ],\n" + 
                "    \"isPartOf\": \"http://fcrepo:8080/fcrepo/rest\",\n" + 
                "    \"wasGeneratedBy\": {\n" + 
                "        \"type\": [\n" + 
                "            \"http://fedora.info/definitions/v4/event#ResourceModification\",\n" + 
                "            \"http://www.w3.org/ns/prov#Activity\"\n" + 
                "        ],\n" + 
                "        \"identifier\": \"urn:uuid:0d97c717-8712-495d-a8b2-500ea962b448\",\n" + 
                "        \"atTime\": \"2018-04-10T14:02:57.812Z\"\n" + 
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
                "    ],\n" + 
                "    \"@context\": {\n" + 
                "        \"prov\": \"http://www.w3.org/ns/prov#\",\n" + 
                "        \"foaf\": \"http://xmlns.com/foaf/0.1/\",\n" + 
                "        \"dcterms\": \"http://purl.org/dc/terms/\",\n" + 
                "        \"xsd\": \"http://www.w3.org/2001/XMLSchema#\",\n" + 
                "        \"type\": \"@type\",\n" + 
                "        \"id\": \"@id\",\n" + 
                "        \"name\": {\n" + 
                "            \"@id\": \"foaf:name\",\n" + 
                "            \"@type\": \"xsd:string\"\n" + 
                "        },\n" + 
                "        \"identifier\": {\n" + 
                "            \"@id\": \"dcterms:identifier\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"isPartOf\": {\n" + 
                "            \"@id\": \"dcterms:isPartOf\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"atTime\": {\n" + 
                "            \"@id\": \"prov:atTime\",\n" + 
                "            \"@type\": \"xsd:dateTime\"\n" + 
                "        },\n" + 
                "        \"wasAttributedTo\": {\n" + 
                "            \"@id\": \"prov:wasAttributedTo\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"wasGeneratedBy\": {\n" + 
                "            \"@id\": \"prov:wasGeneratedBy\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        }\n" + 
                "    }\n" + 
                "}\n";
        
        
        FedoraMessage m = FedoraMessageConverter.convert(jms_text_msg);
        
        assertEquals(FedoraAction.MODIFIED, m.getAction());
        assertEquals("http://fcrepo:8080/fcrepo/rest/submissions/7f/cc/6f/c5/7fcc6fc5-10c8-476d-b537-cf13f20d9be7", m.getResourceURI());
        
        String[] expected_types = new String[] {
            "http://www.w3.org/ns/ldp#Container", 
            "http://oapass.org/ns/pass#Submission", 
            "http://fedora.info/definitions/v4/repository#Resource",
            "http://fedora.info/definitions/v4/repository#Container",
            "http://www.w3.org/ns/ldp#RDFSource",
            "http://www.w3.org/ns/prov#Entity"
        };
        
        assertEquals(to_set(expected_types), to_set(m.getResourceTypes()));
    }
    
    @Test
    public void testConvertDeleted() {
        String jms_text_msg = "{\n" + 
                "    \"id\": \"http://fcrepo:8080/fcrepo/rest/people/bc/1b/6a/e5/bc1b6ae5-f252-4402-aa3a-c240b4c55b40\",\n" + 
                "    \"type\": [\n" + 
                "        \"http://www.w3.org/ns/ldp#Container\",\n" + 
                "        \"http://oapass.org/ns/pass#Person\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Resource\",\n" + 
                "        \"http://fedora.info/definitions/v4/repository#Container\",\n" + 
                "        \"http://www.w3.org/ns/ldp#RDFSource\",\n" + 
                "        \"http://www.w3.org/ns/prov#Entity\"\n" + 
                "    ],\n" + 
                "    \"isPartOf\": \"http://fcrepo:8080/fcrepo/rest\",\n" + 
                "    \"wasGeneratedBy\": {\n" + 
                "        \"type\": [\n" + 
                "            \"http://fedora.info/definitions/v4/event#ResourceDeletion\",\n" + 
                "            \"http://www.w3.org/ns/prov#Activity\"\n" + 
                "        ],\n" + 
                "        \"identifier\": \"urn:uuid:bfd935ef-5d22-4e19-ab26-8ad96db2c0c8\",\n" + 
                "        \"atTime\": \"2018-04-10T14:13:01.939Z\"\n" + 
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
                "    ],\n" + 
                "    \"@context\": {\n" + 
                "        \"prov\": \"http://www.w3.org/ns/prov#\",\n" + 
                "        \"foaf\": \"http://xmlns.com/foaf/0.1/\",\n" + 
                "        \"dcterms\": \"http://purl.org/dc/terms/\",\n" + 
                "        \"xsd\": \"http://www.w3.org/2001/XMLSchema#\",\n" + 
                "        \"type\": \"@type\",\n" + 
                "        \"id\": \"@id\",\n" + 
                "        \"name\": {\n" + 
                "            \"@id\": \"foaf:name\",\n" + 
                "            \"@type\": \"xsd:string\"\n" + 
                "        },\n" + 
                "        \"identifier\": {\n" + 
                "            \"@id\": \"dcterms:identifier\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"isPartOf\": {\n" + 
                "            \"@id\": \"dcterms:isPartOf\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"atTime\": {\n" + 
                "            \"@id\": \"prov:atTime\",\n" + 
                "            \"@type\": \"xsd:dateTime\"\n" + 
                "        },\n" + 
                "        \"wasAttributedTo\": {\n" + 
                "            \"@id\": \"prov:wasAttributedTo\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        },\n" + 
                "        \"wasGeneratedBy\": {\n" + 
                "            \"@id\": \"prov:wasGeneratedBy\",\n" + 
                "            \"@type\": \"@id\"\n" + 
                "        }\n" + 
                "    }\n" + 
                "}\n";
        
        FedoraMessage m = FedoraMessageConverter.convert(jms_text_msg);
        
        assertEquals(FedoraAction.DELETED, m.getAction());
        assertEquals("http://fcrepo:8080/fcrepo/rest/people/bc/1b/6a/e5/bc1b6ae5-f252-4402-aa3a-c240b4c55b40", m.getResourceURI());
        
        String[] expected_types = new String[] {
            "http://www.w3.org/ns/ldp#Container", 
            "http://oapass.org/ns/pass#Person", 
            "http://fedora.info/definitions/v4/repository#Resource",
            "http://fedora.info/definitions/v4/repository#Container",
            "http://www.w3.org/ns/ldp#RDFSource",
            "http://www.w3.org/ns/prov#Entity"
        };
        
        assertEquals(to_set(expected_types), to_set(m.getResourceTypes()));
    }
}
