package org.dataconservancy.pass.indexer;

import java.io.IOException;

import org.apache.activemq.ActiveMQConnectionFactory;

// Load configuration from system properties or environment variables.
// Then start the Fedora indexer service.

public class Main {

    private Main() {
    }

    // Check environment variable and then property.
    // Key must exist.
    private static String get_config(final String key) {
        String value = get_config(key, null);

        if (value == null) {
            System.err.println("Required configuration property is missing: " + key);
            System.exit(1);
        }

        return value;
    }

    // Check environment variable and then property
    private static String get_config(final String key, final String default_value) {
        String value = System.getenv().get(key);

        if (value == null) {
            value = System.getProperty(key);
        }

        if (value == null) {
            return default_value;
        }

        return value;
    }

    public static void main(String[] args) throws IOException {
        try (FedoraIndexerService serv = new FedoraIndexerService()) {
            serv.setJmsConnectionFactory(new ActiveMQConnectionFactory(
                get_config("PI_FEDORA_JMS_USER", null),
                get_config("PI_FEDORA_JMS_PASSWORD", null),
                get_config("PI_FEDORA_JMS_BROKER")));
            serv.setJmsQueue(get_config("PI_FEDORA_JMS_QUEUE"));
            serv.setElasticsearchIndexUrl(get_config("PI_ES_INDEX"));
            serv.setElasticsearchIndexConfig(get_config("PI_ES_CONFIG", null));
            serv.setFedoraUser(get_config("PI_FEDORA_USER"));
            serv.setFedoraPass(get_config("PI_FEDORA_PASS"));
            serv.setAllowedTypePrefix(get_config("PI_TYPE_PREFIX"));

            System.out.println("Starting Fedora indexing service.");

            serv.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                System.out.println("Fedora index service interrupted: " + e);
                System.exit(0);
            }
        }
    }
}
