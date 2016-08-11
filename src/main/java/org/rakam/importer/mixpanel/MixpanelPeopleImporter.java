package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import io.rakam.ApiClient;
import io.rakam.ApiException;
import io.rakam.auth.ApiKeyAuth;
import io.rakam.client.api.UserApi;
import io.rakam.client.model.SchemaField;
import io.rakam.client.model.UserCreateUsers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Map;

@Command(name = "import-people", description = "Mixpanel importer")
public class MixpanelPeopleImporter implements Runnable {
    private final static Logger LOGGER = Logger.get(MixpanelEventImporter.class);

    private final static ObjectMapper mapper = new ObjectMapper();

    @Option(name="--mixpanel.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name="--mixpanel.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Option(name="--rakam.address", description = "Rakam cluster url", required = true)
    public String rakamAddress;

    @Option(name="--mixpanel.project.timezone", description = "Rakam cluster url", required = true)
    public Integer projectTimezone;

    @Option(name="--rakam.project.write-key", description = "Project", required = true)
    public String rakamWriteKey;

    @Option(name="--schema.file", description = "Mixpanel people schema file")
    public File schemaFile;

    @Option(name="--schema", description = "Mixpanel people schema")
    public String schema;

    @Option(name="--last-seen", description = "Mixpanel people lastSeen filter as date (YYYY-mm-dd)")
    public String lastSeen;

    @Override
    public void run() {
        MixpanelImporter mixpanelEventImporter = new MixpanelImporter(apiKey, apiSecret);

        if(schemaFile != null) {
            if(schema != null) {
                throw new IllegalArgumentException("Only one of schema and schemaFile can be set");
            }

            try {
                schema = new String(Files.readAllBytes(Paths.get(schemaFile.getPath())), "UTF-8");
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        Map<String, SchemaField> fields;
        if(schema != null) {
            try {
                fields = mapper.readValue(schema, new TypeReference<Map<String, SchemaField>>() {});
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else {
            fields = null;
        }

        LocalDate lastSeenDate;
        if(lastSeen != null) {
            lastSeenDate = LocalDate.parse(lastSeen);
        } else {
            lastSeenDate = null;
        }

        ApiClient apiClient = new ApiClient();
        ((ApiKeyAuth) apiClient.getAuthentication("write_key")).setApiKey(rakamWriteKey);

        apiClient.setBasePath(rakamAddress);
        UserApi userApi = new UserApi(apiClient);

        try {
            mixpanelEventImporter.importPeopleFromMixpanel(fields, lastSeenDate,
                    (users) -> {
                        UserCreateUsers createReq = new UserCreateUsers();
                        createReq.setUsers(users);

                        try {
                            userApi.createUsers(createReq);
                        } catch (ApiException e) {
                            LOGGER.error(e);
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
