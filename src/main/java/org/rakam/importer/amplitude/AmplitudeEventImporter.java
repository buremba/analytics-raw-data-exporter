package org.rakam.importer.amplitude;

import com.google.common.io.ByteStreams;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import io.rakam.ApiClient;
import io.rakam.ApiException;
import io.rakam.auth.ApiKeyAuth;
import io.rakam.client.api.CollectApi;
import io.rakam.client.model.EventContext;
import io.rakam.client.model.EventList;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(name = "import-events", description = "Amplitude importer")
public class AmplitudeEventImporter
        implements Runnable
{
    private final static Logger LOGGER = Logger.get(AmplitudeEventImporter.class);

    @Option(name = "--amplitude.api-key", description = "Api key", required = true)
    public String apiKey;

    @Option(name = "--amplitude.api-secret", description = "Api secret", required = true)
    public String apiSecret;

    @Option(name = "--rakam.project.master-key", description = "Project", required = true)
    public String rakamMasterKey;

    @Option(name = "--rakam.address", description = "Rakam cluster url", required = true)
    public String rakamAddress;

    @Option(name = "--start", description = "Amplitude event start date")
    public String startDate;

    @Option(name = "--end", description = "Amplitude event end date")
    public String endDate;

    @Option(name = "--duration", description = "Amplitude event import duration")
    public String duration;

    @Option(name = "--rakam-batch-size", description = "Mixpanel event import duration")
    public int rakamBatchSize = 500000;

    @Option(name = "--amplitude-batch-size", description = "Mixpanel event import duration")
    public int amplitudeBatchSize = 10_000_000;

    @Override
    public void run()
    {
        AmplitudeImporter amplitudeImporter = new AmplitudeImporter(apiKey, apiSecret);
        ApiClient apiClient = new ApiClient();
        ((ApiKeyAuth) apiClient.getAuthentication("write_key")).setApiKey(rakamMasterKey);

        apiClient.setBasePath(rakamAddress);
        CollectApi eventApi = new CollectApi(apiClient);

        LocalDate start = null, end = null;
        if (startDate != null) {
            start = LocalDate.parse(startDate);
        }
        if (endDate != null) {
            end = LocalDate.parse(endDate);
        }
        if (duration != null) {
            Period parse = Period.parse(this.duration);
            if (!parse.minusYears(3).isNegative()) {
                throw new IllegalArgumentException("The importer supports only 3 years of historical data from Amplitude");
            }

            if (parse.isZero()) {
                throw new IllegalArgumentException("interval is invalid.");
            }

            if (endDate == null && startDate != null) {
                end = start.plus(parse);
            }
            else if (endDate != null && startDate != null) {
                throw new IllegalArgumentException("duration must not set when startDate and endDate is set");
            }
            else if (endDate != null && startDate == null) {
                start = end.minus(parse);
            }
            else {
                // if current day is included, we may not find the offset if unique event id is not imported.
                end = LocalDate.now();
                start = end.minus(parse);
            }
        }

        if (end == null) {
            end = LocalDate.now();
        }

        if (start == null) {
            throw new IllegalArgumentException("Duration must be set");
        }

        final LocalDate finalStart = start;
        final LocalDate finalEnd = end;

        try {

            Map.Entry<List<Map.Entry<LocalDateTime, LocalDateTime>>, Long> result = amplitudeImporter.getTasks(finalStart.atStartOfDay(), finalEnd.atStartOfDay(), amplitudeBatchSize);

            LOGGER.info("We have %d tasks for fetching %d events from Amplitude.", result.getKey().size(), result.getValue());
            for (int i = 0; i < result.getKey().size(); i++) {
                if (i > 0) {
                    LOGGER.info("%d tasks are done. Remaining tasks: %d.", i, result.getKey().size() - i);
                }
                Map.Entry<LocalDateTime, LocalDateTime> task = result.getKey().get(i);
                amplitudeImporter.importEvents(task.getKey(), task.getValue(),
                        (events) -> {
//                            if (true) { return; }
                            EventContext context = new EventContext();
                            context.setApiKey(rakamMasterKey);

                            EventList eventList = new EventList();
                            eventList.setApi(context);
                            eventList.setEvents(events);

                            try {
                                eventApi.bulkEvents(eventList);
                            }
                            catch (ApiException e) {
                                LOGGER.error(e);
                            }
                        }, rakamBatchSize);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static InputStream generateRequest(String apiKey, String secretKey, Map<String, String> build)
            throws IOException
    {
        String encodedUrlString = build.entrySet().stream().map(e -> {
            try {
                return e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8");
            }
            catch (UnsupportedEncodingException e1) {
                return e.getValue();
            }
        }).collect(Collectors.joining("&"));
        HttpsURLConnection connection = (HttpsURLConnection) new URL("https://amplitude.com/api/2/export?" + encodedUrlString).openConnection();

        try {
            connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((apiKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8)));
            return connection.getInputStream();
        }
        catch (IOException e) {
            if (connection.getResponseCode() == 404) {
                return null;
            }
            throw new RuntimeException(new String(connection.getResponseCode() + " -> " + ByteStreams.toByteArray(connection.getErrorStream())), e);
        }
    }
}
