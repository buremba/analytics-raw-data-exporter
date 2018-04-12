package org.rakam.importer.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.rakam.importer.Event;

import javax.net.ssl.HttpsURLConnection;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Scanner;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.lang.String.format;
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
    {
        HttpsURLConnection connection = null;

        try {
            connection = (HttpsURLConnection) new URL("https://amplitude.com/api/2/events/segmentation?e={%22event_type%22:%22_all%22}&i=30&m=totals&start=" + DATE_FORMAT.format(startDate) + "&end=" + DATE_FORMAT.format(endDate)).openConnection();

            connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((apiKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8)));
            JsonNode node = mapper.readTree(toByteArray(connection.getInputStream()));
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
//                if(startThis.isBefore(startDate) || endThis.isAfter(endDate)) {
//                    continue;
//                }

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
            try {
                String message = connection.getErrorStream() != null ? new String(toByteArray(connection.getErrorStream())) : "Error while sending request to Amplitude";
                throw new RuntimeException(message, e);
            }
            catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    public void downloadEvents(File directory, LocalDateTime startDate, LocalDateTime endDate)
    {
        downloadEvents(directory, startDate, endDate, 6);
    }

    public int getTotalEvents(File file)
    {
        ZipFile zipFile = null;

        try {
            zipFile = new ZipFile(file);
            int i = 0;

            Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                InputStream zis = zipFile.getInputStream(entry);

                if (entry.isDirectory()) {
                    throw new IllegalStateException();
                }

                try {
                    InputStream gzipStream = new GZIPInputStream(zis, Ints.checkedCast(entry.getSize()));
                    LineNumberReader scanner = new LineNumberReader(new InputStreamReader(gzipStream));

                    String line = scanner.readLine();
                    while (line != null) {
                        i++;
                        line = scanner.readLine();
                    }
                }
                catch (Exception e) {
                    LOGGER.error(e, "Error while reading file from archive");
                    continue;
                }
            }

            return i;
        }
        catch (Exception e) {
            LOGGER.error(e, format("Error while reading archive %s", file.getName()));
            return 0;
        }
        finally {
            if (zipFile != null) {
                try {
                    zipFile.close();
                }
                catch (IOException e) {
                    //
                }
            }
        }
    }

    public void downloadEvents(File directory, LocalDateTime startDate, LocalDateTime endDate, int tryCount)
    {
        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("start", DATE_FORMAT.format(startDate))
                .put("end", DATE_FORMAT.format(endDate)).build();

        LOGGER.info("Downloading data from Amplitude for time period %s and %s..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));

        byte[] buffer = new byte[64 * 1024];

        InputStream input = null;
        try {
            input = generateRequest(apiKey, secretKey, build, 3);
            if (input == null) {
                return;
            }

            File file = new File(directory, format("%s-%s.zip", startDate.toString(), endDate.toString()));
            OutputStream output = new FileOutputStream(file);
            try {
                int bytesRead;
                while ((bytesRead = input.read(buffer)) != -1) {
                    output.write(buffer, 0, bytesRead);
                }
            }
            finally {
                output.close();
            }
        }
        catch (IOException e) {
            if (tryCount == 0) {
                throw new RuntimeException(e);
            }
            downloadEvents(directory, startDate, endDate, tryCount - 1);
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    if (tryCount == 0) {
                        throw new RuntimeException(e);
                    }

                    downloadEvents(directory, startDate, endDate, tryCount - 1);
                }
            }
        }
    }

    public void importEvents(File file, Supplier<Event> iterator)
    {
        ZipFile zipFile;
        try {
            zipFile = new ZipFile(file);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

        try {
            while (entries.hasMoreElements()) {
                ZipArchiveEntry entry = entries.nextElement();
                InputStream zis = zipFile.getInputStream(entry);

                try {
                    InputStream gzipStream = new GZIPInputStream(zis, Ints.checkedCast(entry.getSize()));
                    LineNumberReader scanner = new LineNumberReader(new InputStreamReader(gzipStream));
                    String line = scanner.readLine();
                    while (line != null) {
                        AmplitudeEvent read = null;
                        try {
                            read = mapper.readValue(line, AmplitudeEvent.class);
                        }
                        catch (IOException e) {
                            LOGGER.warn(e, "Invalid JSON");
                        }
                        line = scanner.readLine();

                        if (read.is_attribution_event) {
                            read.user_properties.isEmpty();
                            continue;
                        }

                        Event event = iterator.get();

                        event.collection = read.event_type != null ? read.event_type : read.amplitude_event_type;

                        Map<String, Object> record = new HashMap<>();
                        event.properties = record;

                        if (read.revenue != null) {
                            record.put("revenue", read.device_carrier);
                        }

                        record.put("_device_carrier", read.device_carrier);
                        record.put("_city", read.city);
                        record.put("_region", read.region);
                        record.put("_country", read.country);
                        record.put("_user", read.user_id);
                        record.put("_id", read.uuid);
                        record.put("_time", read.event_time);
                        record.put("_platform", read.platform);
                        record.put("_os_version", read.os_version);
                        record.put("_os", read.os_name);
                        record.put("__ip", read.ip_address);
                        record.put("_library", read.library);
                        record.put("_device_family", read.device_type);
                        record.put("_device_manufacturer", read.device_manufacturer);
                        record.put("_longitude", read.location_lng);
                        record.put("_latitude", read.location_lat);
                        record.put("_os_name", read.os_name);
                        record.put("_device_brand", read.device_brand);
                        record.put("_device_id", read.device_id);
                        record.put("_language", read.language);
                        record.put("_device_model", read.device_model);
                        record.put("_adid", read.adid);
                        record.put("_session_id", read.session_id);
                        record.put("_device_family", read.device_family);
                        record.put("_idfa", read.idfa);
                        record.put("_dma", read.dma);

                        for (Map.Entry<String, Object> item : read.event_properties.entrySet()) {
                            record.put(item.getKey(), item.getValue());
                        }
                    }
                }
                catch (Exception e) {
                    LOGGER.warn(e, "Corrupted sub archive. skipping..");
                }
            }
        }
        catch (Exception e) {
            LOGGER.error(e, "Error while reading file");
        } finally {
            try {
                zipFile.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}