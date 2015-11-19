package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.IOException;

@Command(name = "explain-people", description = "Mixpanel people schema explainer")
public class MixpanelPeopleExplainer implements Runnable {
    private final static ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Option(name="--mixpanel.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name="--mixpanel.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Override
    public void run() {
        MixpanelImporter mixpanel = new MixpanelImporter(apiKey, apiSecret);
        try {
            System.out.println(mapper.writeValueAsString(mixpanel.mapPeopleFields()));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
