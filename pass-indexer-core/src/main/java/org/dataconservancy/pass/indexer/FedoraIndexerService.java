package org.dataconservancy.pass.indexer;

import java.io.IOException;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Setup a handler that reads Fedora events from a JMS queue and updates an
 * Elasticsearch index in response.
 */
public class FedoraIndexerService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FedoraIndexerService.class);

    private JmsClient jms_client;
    private ConnectionFactory jms_con_fact;
    private String jms_queue;
    private String allowed_type_prefix;
    private String elasticsearch_index_url;
    private String fedora_user;
    private String fedora_pass;
    private String elasticsearch_index_config;
    
    public void setJmsConnectionFactory(ConnectionFactory conf_fact) {
        this.jms_con_fact = conf_fact;
    }

    public void setJmsQueue(String jms_queue) {
        this.jms_queue = jms_queue;
    }

    public void setAllowedTypePrefix(String prefix) {
        this.allowed_type_prefix = prefix;
    }

    public void setElasticsearchIndexUrl(String elasticsearch_index_url) {
        this.elasticsearch_index_url = elasticsearch_index_url;
    }
    
    public void setElasticsearchIndexConfig(String elasticsearch_index_config) {
        this.elasticsearch_index_config = elasticsearch_index_config;
    }

    public void setFedoraUser(String fedora_user) {
        this.fedora_user = fedora_user;
    }

    public void setFedoraPass(String fedora_pass) {
        this.fedora_pass = fedora_pass;
    }

    private boolean should_handle(FedoraMessage fedora_msg) {
        for (String type : fedora_msg.getResourceTypes()) {
            if (type.startsWith(allowed_type_prefix)) {
                return true;
            }
        }

        return false;
    }

    public void start() throws IOException {
        if (elasticsearch_index_url == null) {
            String msg = "Index location not specified.";
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        if (elasticsearch_index_config == null) {
            String msg = "Index configuration not specified.";
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        jms_client = new JmsClient(jms_con_fact);

        ElasticSearchIndexer es = new ElasticSearchIndexer(elasticsearch_index_url, elasticsearch_index_config, fedora_user, fedora_pass);
        
        jms_client.listen(jms_queue, msg -> {
            try {
                FedoraMessage fedora_msg = FedoraMessageConverter.convert(msg);

                boolean should_handle = should_handle(fedora_msg);

                if (should_handle) {
                    es.handle(fedora_msg);
                } else {
                	LOG.debug("Ignore Fedora message without known RDF type: " + fedora_msg);
                }
            } catch (IOException | JMSException e) {
                throw new RuntimeException(e);
            }
        });

        LOG.info("Started listening on jms queue " + jms_queue);
        LOG.info("Elasticsearch index: " + elasticsearch_index_url);
    }

    @Override
    public void close() {
        if (jms_client != null) {
            LOG.info("Shutting down JMS client");
            jms_client.close();
        }
    }

    // Needed for testing
    protected JmsClient getJmsClient() {
        return jms_client;
    }
}
