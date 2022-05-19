package org.dataconservancy.pass.indexer.reindex;

import static com.openpojo.reflection.impl.PojoClassFactory.enumerateClassesByExtendingType;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.openpojo.reflection.PojoClass;
import org.dataconservancy.pass.client.PassClient;
import org.dataconservancy.pass.client.PassClientFactory;
import org.dataconservancy.pass.indexer.ElasticSearchIndexer;
import org.dataconservancy.pass.model.PassEntity;

// Load configuration from system properties or environment variables.
// Then start the Fedora indexer service.

public class Main {

    private Main() {
    }

    @SuppressWarnings("unchecked")
    public static final Collection<Class<? extends PassEntity>> PASS_TYPES = enumerateClassesByExtendingType(
        "org.dataconservancy.pass.model", PassEntity.class, null).stream().map(PojoClass::getClazz)
                                                                 .map(cls -> (Class<? extends PassEntity>) cls)
                                                                 .collect(Collectors.toList());

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
        String index = get_config("PI_ES_INDEX", "http://localhost:9200/pass/");
        System.out.println("Using index " + index);
        ElasticSearchIndexer es = new ElasticSearchIndexer(
            index,
            get_config("PI_ES_CONFIG",
                       "https://raw.githubusercontent.com/OA-PASS/pass-data-model/master/src/main/resources/esconfig" +
                       "-3.5.json"),
            get_config("PI_FEDORA_USER", "fedoraAdmin"), get_config("PI_FEDORA_PASS", "moo"));

        PassClient client = PassClientFactory.getPassClient();

        Function<Class<? extends PassEntity>, Stream<URI>> lister = entityType -> {
            ResultStager stager = new ResultStager();
            client.processAllEntities(stager, entityType);
            return stager.entries();
        };

        Function<URI, String> task = uri -> {
            try {
                return es.update_document(uri.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        };

        try (SqliteRunner runner = getRunner(args)) {
            runner.process(lister, task);

            while (runner.errorCount() > 0) {
                int prev = runner.errorCount();
                runner.clearErrors();
                runner.process(lister, task);

                if (runner.errorCount() >= prev) {
                    throw new RuntimeException(String.format("Cannot recover from %d errors", prev));
                }
            }
        }

        System.out.println("Finished OK!");
    }

    private static SqliteRunner getRunner(String[] args) {
        if (args.length == 0) {
            return new SqliteRunner(PASS_TYPES);
        } else if (args.length == 1) {
            return new SqliteRunner(args[0]);
        }

        throw new RuntimeException("Expected zero or one cli args");
    }
}
