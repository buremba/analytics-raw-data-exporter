package org.rakam.importer.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.rakam.importer.Event;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.rakam.importer.amplitude.AmplitudeEventImporter.generateRequest;
import static org.rakam.importer.amplitude.AmplitudeEventImporter.mapper;

public class AmplitudeImporter
{
    DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'H");

    private final static Logger LOGGER = Logger.get(AmplitudeImporter.class);

    private final String apiKey;
    private final String secretKey;

    public AmplitudeImporter(String apiKey, String secretKey)
    {
        this.apiKey = apiKey;
        this.secretKey = secretKey;
    }

    public Map.Entry<List<Map.Entry<LocalDateTime, LocalDateTime>>, Long> getTasks(LocalDateTime startDate, LocalDateTime endDate, int maxBatchSize)
            throws IOException
    {
        HttpsURLConnection connection = (HttpsURLConnection) new URL("https://amplitude.com/api/2/events/segmentation?e={%22event_type%22:%22_all%22}&i=30&m=totals&start=" + DATE_FORMAT.format(startDate) + "&end=" + DATE_FORMAT.format(endDate)).openConnection();

        try {
            connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((apiKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8)));
            JsonNode node = mapper.readTree(ByteStreams.toByteArray(connection.getInputStream()));
            Iterator<JsonNode> values = node.get("data").get("series").get(0).elements();
            long total = node.get("data").get("seriesCollapsed").get(0).get(0).get("value").asLong();
            Iterator<JsonNode> keys = node.get("data").get("xValues").elements();

            List<Map.Entry<LocalDateTime, LocalDateTime>> objects = new ArrayList<>();

            LocalDateTime start = startDate;
            int stackedEvents = 0;

            while (keys.hasNext()) {
                long totalEvents = values.next().asLong();

                LocalDateTime startThis = LocalDate.parse(keys.next().asText()).atStartOfDay();
                LocalDateTime endThis = startThis.plusMonths(1).toLocalDate().atStartOfDay();

                if (stackedEvents + totalEvents < maxBatchSize) {
                    if (totalEvents == 0 && stackedEvents == 0) {
                        start = endThis;
                    }
                    stackedEvents += totalEvents;
                    continue;
                }

                if (stackedEvents > 0) {
                    objects.add(new SimpleImmutableEntry<>(start, startThis.minusHours(1)));
                    stackedEvents = 0;
                }

                long incrementBy = Math.max(30 / ((totalEvents / maxBatchSize) + 1), 1);

                while (startThis.isBefore(endThis)) {
                    LocalDateTime tempEnd = startThis.plusDays(incrementBy);
                    if (tempEnd.isAfter(endThis)) {
                        tempEnd = endThis;
                    }
                    objects.add(new SimpleImmutableEntry<>(startThis, tempEnd.minusHours(1)));
                    startThis = start = tempEnd;
                }
            }

            if (stackedEvents > 0) {
                objects.add(new SimpleImmutableEntry<>(start, endDate));
            }

            return new SimpleImmutableEntry<>(objects, total);
        }
        catch (IOException e) {
            throw new RuntimeException(new String(ByteStreams.toByteArray(connection.getErrorStream())), e);
        }
    }

    public void importEvents(LocalDateTime startDate, LocalDateTime endDate, Consumer<List<Event>> consumer, int rakamBatch)
            throws IOException
    {
        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("start", DATE_FORMAT.format(startDate))
                .put("end", DATE_FORMAT.format(endDate)).build();

        LOGGER.info("Sending export request to Amplitude for time period %s and %s..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));
        InputStream export = generateRequest(apiKey, secretKey, build);
        if (export == null) {
            return;
        }

        Event[] batchRecords = new Event[rakamBatch];
        for (int i = 0; i < batchRecords.length; i++) {
            Event event = batchRecords[i] = new Event();
            event.properties = new HashMap<>();
        }

        int idx = 0, batch = 0;

        ZipArchiveInputStream zis = new ZipArchiveInputStream(export, "UTF-8", true, true);
        ArchiveEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            if (entry.isDirectory()) {
                throw new IllegalStateException();
            }

            LOGGER.info("Opening file %s", entry.getName());

            InputStream gzipStream = new GZIPInputStream(zis);
            Scanner scanner = new Scanner(gzipStream);
            scanner.useDelimiter("\n");

            while (scanner.hasNext()) {
                String next = scanner.next();
                AmplitudeEvent read = mapper.readValue(next, AmplitudeEvent.class);
                if (read.is_attribution_event) {
                    read.user_properties.isEmpty();
                    continue;
                }

                Event event = batchRecords[idx++];

                event.collection = read.event_type != null ? read.event_type : read.amplitude_event_type;

                Map<String, Object> record = new HashMap<>();
                event.properties = record;

                if (read.revenue != null) {
                    record.put("revenue", read.device_carrier);
                }

                record.put("device_carrier", read.device_carrier);
                record.put("_city", read.city);
                record.put("_region", read.region);
                record.put("_country", read.country);
                record.put("_user", read.user_id);
                record.put("id", read.uuid);
                record.put("_time", read.event_time);
                record.put("_platform", read.platform);
                record.put("_os_version", read.os_version);
                record.put("__ip", read.ip_address);
                record.put("_library", read.library);
                record.put("device_type", read.device_type);
                record.put("device_manufacturer", read.device_manufacturer);
                record.put("_longitude", read.location_lng);
                record.put("_latitude", read.location_lat);
                record.put("os_name", read.os_name);
                record.put("device_brand", read.device_brand);
                record.put("os_name", read.os_name);
                record.put("device_id", read.device_id);
                record.put("language", read.language);
                record.put("device_model", read.device_model);
                record.put("adid", read.adid);
                record.put("session_id", read.session_id);
                record.put("device_family", read.device_family);
                record.put("idfa", read.idfa);
                record.put("dma", read.dma);

                for (Map.Entry<String, Object> item : read.event_properties.entrySet()) {
                    record.put(item.getKey(), item.getValue());
                }

                if (idx == batchRecords.length) {
                    LOGGER.info("Sending event batch to Rakam. Offset: %d, Current Batch: %d",
                            batch * batchRecords.length, idx);
                    batch++;
                    idx = 0;
                    consumer.accept(Arrays.asList(batchRecords));
                }
            }
        }

        if (idx > 0) {
            LOGGER.info("Sending last event batch to Rakam. Offset: %d, Current Batch: %d",
                    batch * batchRecords.length, idx);
            consumer.accept(Arrays.asList(Arrays.copyOfRange(batchRecords, 0, idx)));
        }
    }
}