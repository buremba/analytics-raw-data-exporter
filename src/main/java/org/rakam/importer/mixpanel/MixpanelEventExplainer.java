package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "explain-events", description = "Mixpanel importer")
public class MixpanelEventExplainer implements Runnable {
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
            System.out.println(mapper.writeValueAsString(mixpanel.mapEventFields()));
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}
