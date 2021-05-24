package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * IT that depends on docker setup in docker-compose.yml.
 * 
 * The tests do not clean up after themselves, but are written so that they should generally succeed anyway.
 */
public class PassIndexerIT implements IndexerConstants {
	private static final String fedora_base_uri = "http://localhost:8080/fcrepo/rest/";
	private static final String es_uri = "http://localhost:9200/_search";
	private static final MediaType JSON_LD = MediaType.parse("application/ld+json; charset=utf-8");
	private static final MediaType JSON_MERGE_PATCH = MediaType.parse("application/merge-patch+json; charset=utf-8");
	private static final int WAIT_TIME = 20 * 1000;

	private static FedoraIndexerService serv;
	private static OkHttpClient client;
	private static String fedora_cred;

	@BeforeClass
	public static void setup() throws Exception {
		serv = new FedoraIndexerService();

		serv.setJmsConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:61616"));
		serv.setJmsQueue("fedora");
		serv.setElasticsearchIndexUrl("http://localhost:9200/pass/");
		serv.setElasticsearchIndexConfig("/esindex.json");
		serv.setFedoraUser("fedoraAdmin");
		serv.setFedoraPass("moo");
		serv.setAllowedTypePrefix("http://oapass.org/ns/pass#");

		serv.start();

		client = new OkHttpClient();

		fedora_cred = Credentials.basic("fedoraAdmin", "moo");
	}

	@AfterClass
	public static void cleanup() {
		serv.close();
	}

	// Create a Fedora resource and return the assigned URI.
	private String post_fedora_resource(String container_name, JSONObject content) throws Exception {
		String uri = fedora_base_uri + container_name;

		RequestBody body = RequestBody.create(JSON_LD, content.toString());

		Request post = new Request.Builder().url(uri).header("Authorization", fedora_cred).post(body).build();

		try (Response response = client.newCall(post).execute()) {
			if (response.isSuccessful()) {
				return response.header("Location");
			} else {
				throw new IOException("Failed to create: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Update a Fedora resource
	private void patch_fedora_resource(String uri, JSONObject content) throws Exception {
		RequestBody body = RequestBody.create(JSON_MERGE_PATCH, content.toString());

		Request put = new Request.Builder().url(uri).header("Authorization", fedora_cred)
				.header("Prefer", FEDORA_PREFER_HEADER).patch(body).build();

		try (Response response = client.newCall(put).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Failed to update: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Delete a Fedora resource, leaving a tombstone.
	private void delete_fedora_resource(String uri) throws Exception {
		Request delete = new Request.Builder().url(uri).header("Authorization", fedora_cred).delete().build();

		try (Response response = client.newCall(delete).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Failed to delete: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Execute an Elasticsearch query and return the result
	private JSONObject execute_es_query(JSONObject query) throws Exception {
		RequestBody body = RequestBody.create(JSON, query.toString());

		Request post = new Request.Builder().url(es_uri).post(body).build();

		try (Response response = client.newCall(post).execute()) {
			if (response.isSuccessful()) {
				return new JSONObject(response.body().string());
			} else {
				throw new IOException("Failed to execute query: " + query + ", response: " + response.body().string());
			}
		}
	}

	private JSONObject create_pass_object(String type) {
		JSONObject result = new JSONObject();

		// Must set @id to ""
		result.put("@id", "");
		result.put("@context", "https://oa-pass.github.io/pass-data-model/src/main/resources/context-3.2.jsonld");
		result.put("@type", type);

		return result;
	};

	private JSONObject create_term_query(String field, String value) {
		JSONObject query = new JSONObject();
		JSONObject term_match = new JSONObject();
		JSONObject term_value = new JSONObject();

		term_value.put(field, value);
		term_match.put("term", term_value);
		query.put("query", term_match);

		return query;
	}
	
	private JSONObject create_completion_query(String field, String prefix, JSONObject context) {
		JSONObject query = new JSONObject();
		JSONObject suggest= new JSONObject();
		JSONObject field_suggest = new JSONObject();
		JSONObject completion = new JSONObject();
		
		completion.put("field", field + "_suggest");
		completion.put("size", 100);
		
		if (context != null) {
			completion.put("context", context);
		}
		
		field_suggest.put("prefix", prefix);
		field_suggest.put("completion", completion);		

		suggest.put(field, field_suggest);
		
		query.put("suggest", suggest);

		return query;
	}

	private JSONObject execute_es_id_query(String uri) throws Exception {
		JSONObject result = execute_es_query(create_term_query("@id", uri));

		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		if (hits.length() != 0 && hits.length() != 1) {
			assertTrue("A fedora resource should have at most one Es document: " + uri, false);
		}

		return result;
	}

	private JSONObject get_indexed_fedora_resource(String uri) throws Exception {
		JSONObject result = execute_es_id_query(uri);

		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		if (hits.length() == 0) {
			assertTrue("No Elasticsearch document: " + uri, false);
		}

		return hits.getJSONObject(0).getJSONObject("_source");
	}

	private boolean is_fedora_resource_indexed(String uri) throws Exception {
		JSONObject result = execute_es_id_query(uri);
		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		return hits.length() == 1;
	}

	private LocalDateTime parseISODateTime(String s) {
        try {

            return LocalDateTime.from(DateTimeFormatter.ISO_DATE_TIME.parse(s));
        } catch (DateTimeParseException e) {
            assertTrue("Invalid date " + s + ": " + e, false);
            return null;
        }
	}
	
	// Check that Elasticsearch document is created for a Fedora User resource.
	// When the Fedora resource is updated, so is the document.
	@Test
	public void testCreateAndModify() throws Exception {
		JSONObject user = create_pass_object("User");
		user.put("username", "cow1");
		user.put("email", "moo@example.org");

		String uri = post_fedora_resource("users", user);
		Thread.sleep(WAIT_TIME);

		LocalDateTime created;
		LocalDateTime modified;
		
		// Check Es document
		{
			JSONObject result = get_indexed_fedora_resource(uri);

			user.put("@id", uri);

			user.keySet().forEach(key -> {
				assertEquals(result.get(key), user.get(key));
			});
			
	         assertTrue(result.has(CREATED_FIELD));
	         assertTrue(result.has(MODIFIED_FIELD));
	            
	         created = parseISODateTime(result.getString(CREATED_FIELD));
	         modified = parseISODateTime(result.getString(MODIFIED_FIELD));
	         
	         assertTrue(created.equals(modified) || created.isBefore(modified));
		}

		// Update the Fedora resource
		user.put("displayName", "Bob");

		patch_fedora_resource(uri, user);
		Thread.sleep(WAIT_TIME);

		// Check the Es document
		{
			JSONObject result = get_indexed_fedora_resource(uri);

			user.keySet().forEach(key -> {
				assertEquals(result.get(key), user.get(key));
			});
			
			
            assertTrue(result.has(CREATED_FIELD));
            assertTrue(result.has(MODIFIED_FIELD));
            
            
            LocalDateTime updated_created = parseISODateTime(result.getString(CREATED_FIELD));
            LocalDateTime updated_modified = parseISODateTime(result.getString(MODIFIED_FIELD));            
            
            assertEquals(created, updated_created);
            assertNotEquals(modified, updated_modified);
            assertTrue(modified.isBefore(updated_modified));
		}
	}

	// Create and then delete a policy object.
	// The Es document for the object is also deleted.
	@Test
	public void testDelete() throws Exception {
		
		JSONObject policy = create_pass_object("Policy");
		policy.put("title", "The best policy");

		String uri = post_fedora_resource("policies", policy);
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(uri));
		
		delete_fedora_resource(uri);
		Thread.sleep(WAIT_TIME);

		assertFalse(is_fedora_resource_indexed(uri));
	}

	private String get_fedora_resource_path(String uri) {
		String marker = "/rest";
		int i = uri.indexOf(marker);
		return uri.substring(i + marker.length());
	}
	
	// Show that Fedora resources can be searched for by resource path.
	// See the Es mapping configuration for pass.
	@Test
	public void testSearchByFedoraURI() throws Exception {
		JSONObject policy = create_pass_object("Policy");
		policy.put("title", "The worst  policy");

		String uri = post_fedora_resource("policies", policy);
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(uri));
		assertTrue(is_fedora_resource_indexed(get_fedora_resource_path(uri)));
	}
	
	// Show that journalName supports completion
	// A completion can start at any word.
	@Test
	public void testCompletion() throws Exception {
		JSONObject journal1 = create_pass_object("Journal");
		journal1.put("journalName", "Cows and other mammals.");

		JSONObject journal2 = create_pass_object("Journal");
		journal2.put("journalName", "Consider the cow.");
		
		JSONObject journal3 = create_pass_object("Journal");
		journal3.put("journalName", "Squirrels, cows, and bunnies");
		
		String uri1 = post_fedora_resource("journals", journal1);
		String uri2 = post_fedora_resource("journals", journal2);
		String uri3 = post_fedora_resource("journals", journal3);
		
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(uri1));
		assertTrue(is_fedora_resource_indexed(uri2));
		assertTrue(is_fedora_resource_indexed(uri3));
	
		JSONObject result = execute_es_query(create_completion_query("journalName", "co", null));
		JSONObject completions = result.getJSONObject("suggest").getJSONArray("journalName").getJSONObject(0);

		// Check that each journal is suggested
		
		List<String> suggested = new ArrayList<>();
		
		completions.getJSONArray("options").forEach(o -> {
			suggested.add(JSONObject.class.cast(o).getJSONObject("_source").getString("@id"));
		});
		
		Arrays.asList(uri1, uri2, uri3).forEach(uri -> {
			assertTrue("Suggestion should contain " + uri, suggested.contains(uri));
		});
	}
	
	// Show that projectName supports completion with a pi category.
	// No longer needed so removed from index config.
	@Test
	@Ignore
	public void testCompletionWithContext() throws Exception {
		// Create PIs
		
		JSONObject pi1 = create_pass_object("User");
		pi1.put("username", "bobafett");
		pi1.put("email", "boba@example.org");
		
		JSONObject pi2 = create_pass_object("User");
		pi2.put("username", "ackbar");
		pi2.put("email", "itsatrap@example.org");

		String pi1_uri = post_fedora_resource("users", pi1);
		String pi2_uri = post_fedora_resource("users", pi2);
		
		Thread.sleep(WAIT_TIME);
		
		// Create grant for each PI

		JSONObject grant1 = create_pass_object("Grant");
		grant1.put("projectName", "Ice Cream: Chocolate or Vanilla?");
		grant1.put("pi", pi1_uri);

		JSONObject grant2 = create_pass_object("Grant");
		grant2.put("projectName", "Making Excellent Ice Cream");
		grant2.put("pi", pi2_uri);

		String grant1_uri = post_fedora_resource("grants", grant1);
		String grant2_uri = post_fedora_resource("grants", grant2);
		
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(pi1_uri));
		assertTrue(is_fedora_resource_indexed(pi2_uri));
		assertTrue(is_fedora_resource_indexed(grant1_uri));
		assertTrue(is_fedora_resource_indexed(grant2_uri));
	
		// The prefix cre matches both grants, context should only match pi1
		JSONObject context = new JSONObject();
		context.put("pi", get_fedora_resource_path(pi1_uri));
		
		JSONObject result = execute_es_query(create_completion_query("projectName", "cre", context));
		JSONObject completions = result.getJSONObject("suggest").getJSONArray("projectName").getJSONObject(0);
		JSONArray options = completions.getJSONArray("options");
				
		assertEquals(1, options.length());
		assertEquals(pi1_uri, options.getJSONObject(0).getJSONObject("_source").getString("pi"));
	}
}
