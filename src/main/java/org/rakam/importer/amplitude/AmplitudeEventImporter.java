package org.rakam.importer.amplitude;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.rakam.importer.Event;

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Command(name = "import-events", description = "Amplitude importer")
public class AmplitudeEventImporter
        implements Runnable
{
    final static ObjectMapper mapper = new ObjectMapper();

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

    @Option(name = "--rakam-batch-size", description = "Amplitude event Rakam request size")
    public int rakamBatchSize = 500000;

    @Option(name = "--amplitude-batch-size", description = "Amplitude event batch request size")
    public int amplitudeBatchSize = 20_000_000;

    @Option(name = "--mode", description = "Mixpanel event import duration")
    public String mode;

    @Option(name = "--out")
    public String outDirectory;

    @Override
    public void run()
    {
        AmplitudeImporter amplitudeImporter = new AmplitudeImporter(apiKey, apiSecret);
        OkHttpClient client = new OkHttpClient.Builder()
                .writeTimeout(0, TimeUnit.MINUTES)
                .connectTimeout(30, TimeUnit.MINUTES)
                .readTimeout(30, TimeUnit.MINUTES).build();

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

        File directory = new File("./files");
        directory.mkdirs();

        if (mode == null || "download".equals(mode)) {
            Map.Entry<List<Map.Entry<LocalDateTime, LocalDateTime>>, Long> result = amplitudeImporter.getTasks(finalStart.atStartOfDay(), finalEnd.atStartOfDay(), amplitudeBatchSize);

            LOGGER.info("We have %d tasks for fetching %d events from Amplitude.", result.getKey().size(), result.getValue());

            for (int i = 0; i < result.getKey().size(); i++) {
                if (i > 0) {
                    LOGGER.info("%d tasks are done. Remaining tasks: %d.", i, result.getKey().size() - i);
                }
                Map.Entry<LocalDateTime, LocalDateTime> task = result.getKey().get(i);
                amplitudeImporter.downloadEvents(directory, task.getKey(), task.getValue());
            }

            LOGGER.info("Downloaded all files, now importing them to Rakam.");
        }

        if ("count".equals(mode)) {
            List<File> orderedFiles = Ordering.natural().onResultOf(new com.google.common.base.Function<File, Comparable>()
            {
                @Nullable
                @Override
                public Comparable apply(@Nullable File file)
                {
                    return file.lastModified();
                }
            }).sortedCopy(Arrays.asList(directory.listFiles()));


            System.out.println(format("%d files in total. processing..", orderedFiles.size()));

            long sum = orderedFiles.parallelStream()
                    .filter(e -> e.getName().endsWith(".zip"))
                    .mapToLong(file -> {
                int totalEvents = amplitudeImporter.getTotalEvents(file);
                System.out.println(format("%s : %d", file.getName(), totalEvents));
                return totalEvents;
            }).sum();

            System.out.println(format("total : %d", sum));
        }

        if (mode == null || "import".equals(mode)) {
            if (outDirectory != null) {
                File file = new File(outDirectory);
                if (file.exists() && file.isFile()) {
                    throw new RuntimeException("--out is a file");
                }
                file.mkdirs();
            }

            List<File> orderedFiles = Ordering.natural().onResultOf(new com.google.common.base.Function<File, Comparable>()
            {
                @Nullable
                @Override
                public Comparable apply(@Nullable File file)
                {
                    return file.lastModified();
                }
            }).sortedCopy(Arrays.asList(directory.listFiles()));

            Event[] batchRecords = new Event[rakamBatchSize];
            for (int i = 0; i < batchRecords.length; i++) {
                batchRecords[i] = new Event();
            }

            AtomicInteger idx = new AtomicInteger();
            AtomicInteger total = new AtomicInteger();
            for (File file : orderedFiles) {
                LOGGER.info("Processing file %s", file.getName());

                amplitudeImporter.importEvents(file,
                        () -> {
                            if (idx.get() == rakamBatchSize) {
                                tryRakam(client, generateContent(total, batchRecords), Integer.MAX_VALUE);
                                total.addAndGet(batchRecords.length);
                                idx.set(0);
                                return batchRecords[0];
                            }

                            return batchRecords[idx.getAndIncrement()];
                        });

                if (outDirectory != null) {
                    try {
                        Files.move(file.toPath(), new File(outDirectory, file.getName()).toPath());
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (idx.get() > 0) {
                tryRakam(client, generateContent(total, batchRecords), Integer.MAX_VALUE);
                total.addAndGet(batchRecords.length);
            }

            LOGGER.info(format("Done importing %d records. Yay!", total.get()));
        }
    }

    private byte[] generateContent(AtomicInteger total, Event[] batchRecords)
    {
        LOGGER.info("Sending event batch to Rakam. Current batch: %d, Previous total: %d", batchRecords.length, total.get());

        HashMap<Object, Object> context = new HashMap<>();
        context.put("api_key", rakamMasterKey);

        HashMap<Object, Object> eventList = new HashMap<>();
        eventList.put("api", context);
        eventList.put("events", batchRecords);
        byte[] content;
        try {
            content = mapper.writeValueAsBytes(eventList);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return content;
    }

    private void tryRakam(OkHttpClient client, byte[] content, int tryCount)
    {
        Response response = null;
        try {
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, content);
            Request request = new Request.Builder()
                    .url(rakamAddress + "/event/bulk")
                    .post(body)
                    .build();

            response = client.newCall(request).execute();
            String string = response.body().string();
            response.close();

            if (response.code() != 200) {
                throw new RuntimeException(string);
            }
        }
        catch (IOException e) {
            if (response != null) {
                response.close();
            }

            if (tryCount == 0) {
                throw new RuntimeException(e);
            }
            tryRakam(client, content, tryCount - 1);
        }
    }

    public static InputStream generateRequest(String apiKey, String secretKey, Map<String, String> build, int tryCount)
    {
        try {
            return generateRequest(apiKey, secretKey, build);
        }
        catch (Exception e) {
            if (tryCount == 0) {
                throw new RuntimeException(e);
            }
            return generateRequest(apiKey, secretKey, build, tryCount - 1);
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
            throw new RuntimeException(new String(connection.getResponseCode() + " -> " + new String(ByteStreams.toByteArray(connection.getErrorStream()))), e);
        }
    }
}
