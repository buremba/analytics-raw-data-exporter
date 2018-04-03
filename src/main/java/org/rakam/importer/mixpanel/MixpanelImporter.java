package org.rakam.importer.mixpanel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.rakam.client.model.Event;
import io.rakam.client.model.SchemaField;
import io.rakam.client.model.User;
import io.rakam.client.model.UserContext;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.rakam.importer.mixpanel.MixpanelEventImporter.*;

public class MixpanelImporter
{
    private final static ObjectMapper mapper = new ObjectMapper();

    private final static List<String> BLACKLIST = ImmutableList.of("$lib_version");

    private final static Logger LOGGER = Logger.get(MixpanelImporter.class);

    private final String apiKey;
    private final String secretKey;

    public MixpanelImporter(String apiKey, String secretKey)
    {
        this.apiKey = apiKey;
        this.secretKey = secretKey;
    }

    private boolean hasAttribute(Map<String, SchemaField> fieldMap, String name)
    {
        return fieldMap.entrySet().stream().anyMatch(a -> a.getValue().getName().equals(name));
    }

    public List<String> getCollections()
            throws IOException
    {
        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("type", "general")
                .put("limit", "4000").build();

        String[] events = mapper.readValue(generateRequestAndParse("events/names", apiKey, secretKey, build), String[].class);
        return Arrays.asList(events);
    }

    public Map<String, SchemaField> mapPeopleFields()
            throws IOException
    {
        byte[] bytes = generateRequestAndParse("engage/properties", apiKey, secretKey, ImmutableMap.of());
        MixpanelPeopleField read = mapper.readValue(bytes, MixpanelPeopleField.class);
        return read.results.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> createSchemaField(e.getKey(), MixpanelType.fromMixpanelType(e.getValue().type).type)));
    }

    private static class MixpanelPeopleField
    {
        public final Map<String, TopType> results;
        public final String session_id;
        public final String status;

        @JsonCreator
        private MixpanelPeopleField(@JsonProperty("results") Map<String, TopType> results, @JsonProperty("session_id") String session_id, @JsonProperty("status") String status)
        {
            this.results = results;
            this.session_id = session_id;
            this.status = status;
        }
    }

    public Map<String, Table> mapEventFields()
            throws IOException
    {
        ImmutableMap.Builder<String, List<SchemaField>> builder = ImmutableMap.builder();

        for (String event : getCollections()) {
            Map<String, TopType> read;
            try {
                byte[] bytes = toByteArray(generateRequest("events/properties/toptypes", apiKey, secretKey,
                        ImmutableMap.of(
                                "event", event,
                                "type", "general",
                                "limit", "500")));
                read = mapper.readValue(new String(bytes, "UTF-8"), new TypeReference<Map<String, TopType>>()
                {
                });
            }
            catch (IOException e) {
                LOGGER.error(e, String.format("Error while reading event '%s'", event));
                continue;
            }

            List<SchemaField> types = read.entrySet().stream()
                    .map(e -> createSchemaField(e.getKey(), MixpanelType.fromMixpanelType(e.getValue().type).type))
                    .collect(Collectors.toList());
            builder.put(event, types);
        }

        ImmutableMap<String, List<SchemaField>> mixpanelFields = builder.build();

        Map<String, Table> tables = new HashMap<>();
        for (Map.Entry<String, List<SchemaField>> entry : mixpanelFields.entrySet()) {
            Map<String, SchemaField> fieldMap = new HashMap<>();
            tables.put(entry.getKey(), new Table(convertRakamName(entry.getKey()), fieldMap));

            for (SchemaField schemaField : entry.getValue()) {
                if (BLACKLIST.contains(schemaField.getName())) {
                    continue;
                }
                SchemaField f = createSchemaField(convertRakamName(schemaField.getName()), schemaField.getType());
                while (hasAttribute(fieldMap, f.getName())) {
                    f = createSchemaField(f.getName() + "_", schemaField.getType());
                }
                fieldMap.put(schemaField.getName(), f);
            }
            fieldMap.put("time", createSchemaField("_time", SchemaField.TypeEnum.LONG));
            fieldMap.put("$referrer", createSchemaField("_referrer", SchemaField.TypeEnum.LONG));
            fieldMap.put("$referring_domain", createSchemaField("_referring_domain", SchemaField.TypeEnum.LONG));
            fieldMap.put("mp_lib", createSchemaField("mp_lib", SchemaField.TypeEnum.LONG));
            fieldMap.put("distinct_id", createSchemaField("distinct_id", SchemaField.TypeEnum.STRING));
            fieldMap.put("$search_engine", createSchemaField("search_engine", SchemaField.TypeEnum.STRING));
        }

        return tables;
    }

    private static SchemaField createSchemaField(String name, SchemaField.TypeEnum type)
    {
        SchemaField schemaField = new SchemaField();
        schemaField.setName(name);
        schemaField.setType(type);
        return schemaField;
    }

    public static class TopType
    {
        public final long count;
        public final String type;

        @JsonCreator
        public TopType(@JsonProperty("count") long count,
                @JsonProperty("type") String type)
        {
            this.count = count;
            this.type = type;
        }
    }

    public enum MixpanelType
    {
        string("string", SchemaField.TypeEnum.STRING),
        number("number", SchemaField.TypeEnum.DOUBLE),
        bool("boolean", SchemaField.TypeEnum.BOOLEAN),
        datetime("datetime", SchemaField.TypeEnum.TIMESTAMP),
        unknown("unknown", SchemaField.TypeEnum.STRING);

        private final String mixpanelType;
        private final SchemaField.TypeEnum type;

        MixpanelType(String mixpanelType, SchemaField.TypeEnum type)
        {
            this.mixpanelType = mixpanelType;
            this.type = type;
        }

        public static MixpanelType fromMixpanelType(String str)
        {
            for (MixpanelType mixpanelType : values()) {
                if (mixpanelType.mixpanelType.equals(str)) {
                    return mixpanelType;
                }
            }
            throw new IllegalArgumentException("type is now found: " + str);
        }

    }

    public void importEventsFromMixpanel(String mixpanelEventType, String rakamCollection, Map<String, SchemaField> properties, LocalDate startDate, LocalDate endDate, int projectTimezoneOffset, Consumer<List<Event>> consumer)
            throws IOException
    {

        Map<String, String> build = ImmutableMap.<String, String>builder()
                .put("event", "[\"" + mixpanelEventType + "\"]")
                .put("from_date", startDate.format(DateTimeFormatter.ISO_LOCAL_DATE))
                .put("to_date", endDate.format(DateTimeFormatter.ISO_LOCAL_DATE)).build();

        LOGGER.info("Sending export request to Mixpanel for time period %s and %s..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));
        InputStream export = generateRequest("export", apiKey, secretKey, build);
        Scanner scanner = new Scanner(export);
        scanner.useDelimiter("\n");
        Map<String, String> nameCache = properties == null ? new HashMap() : null;

        LOGGER.info("Mixpanel returned events performed between %s and %s. Started processing data and sending to Rakam..",
                ISO_DATE.format(startDate), ISO_DATE.format(endDate));
//        Event[] batchRecords = new Event[10000];

//        for (int i = 0; i < batchRecords.length; i++) {
//            Event event = batchRecords[i] = new Event();
//            event.setCollection(rakamCollection);
//            event.setProperties(new HashMap<>());
//        }

        int idx = 0, batch = 0;
        while (scanner.hasNext()) {
            MixpanelEvent read = mapper.readValue(scanner.next(), MixpanelEvent.class);
//            Map record = (Map) batchRecords[idx++].getProperties();

            for (Map.Entry<String, Object> entry : read.properties.entrySet()) {
//                if (idx == batchRecords.length) {
//                    LOGGER.info("Sending event batch to Rakam. Offset: %d, Current Batch: %d",
//                            batch++ * batchRecords.length, batchRecords.length);
//                    consumer.accept(Arrays.asList(batchRecords));
//                    idx = 0;
//                }

                Object value;
                if (entry.getKey().equals("time")) {
                    // adjust timezone to utc
                    value = ((Number) entry.getValue()).intValue() - projectTimezoneOffset;
                }
                else {
                    value = entry.getValue();
                }

                if (BLACKLIST.contains(entry.getKey())) {
                    continue;
                }

                if (properties != null) {
                    SchemaField schemaField = properties.get(entry.getKey());
                    if (schemaField == null) {
                        continue;
                    }
//                    record.put(schemaField.getName(), value);
                }
                else {
                    String key = nameCache.get(entry.getKey());
                    if (key == null) {
                        key = convertRakamName(entry.getKey());
                        if (key.equals("time")) {
                            key = "_time";
                        }
                        nameCache.put(entry.getKey(), key);
                    }
//                    record.put(key, value);
                }
            }
        }

//        LOGGER.info("Sending last event batch to Rakam. Offset: %d, Current Batch: %d",
//                batch * batchRecords.length, idx);
//        consumer.accept(Arrays.asList(Arrays.copyOfRange(batchRecords, 0, idx)));
    }

    public void importPeopleFromMixpanel(Map<String, SchemaField> properties, LocalDate lastSeen, Consumer<List<User>> consumer)
            throws IOException
    {
        ImmutableMap<String, String> build = lastSeen == null ? ImmutableMap.<String, String>of() :
                ImmutableMap.of("selector", String.format("datetime(%d) >= properties[\"$last_seen\"]", lastSeen.atStartOfDay().toEpochSecond(ZoneOffset.UTC)));

        LOGGER.info("Requesting users " + (lastSeen != null ? "last seen at " + ISO_DATE.format(lastSeen) : "") + "from Mixpanel..");
        EngageResult engage = mapper.readValue(generateRequestAndParse("engage", apiKey, secretKey, build), EngageResult.class);

        LOGGER.info("Mixpanel returned %d people. There are %d people in total. Started to process people data..", engage.results.size(), engage.total);

        do {
            final EngageResult finalEngage = engage;
            List<User> collect = engage.results.stream().map(r -> {
                Map<String, Object> userProps;
                if (properties == null) {
                    userProps = r.properties.entrySet().stream()
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(e -> convertRakamName(e.getKey()), e -> e.getValue()));
                }
                else {
                    userProps = r.properties.entrySet().stream()
                            .filter(e -> properties.containsKey(e.getKey()))
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(e -> properties.get(e.getKey()).getName(), e -> e.getValue()));
                }

                User user = new User();
                user.setId(r.id);
                user.setProperties(userProps);
                return user;
            }).collect(Collectors.toList());
            LOGGER.info("Sending people data batch to Rakam. Current page: %d, Total processed people: %d",
                    finalEngage.page, (finalEngage.page * finalEngage.page_size) + finalEngage.results.size());
            consumer.accept(collect);

            engage = mapper.readValue(generateRequestAndParse("engage", apiKey, secretKey,
                    ImmutableMap.of("session_id", engage.session_id, "page", Long.toString(engage.page + 1))), EngageResult.class);
        }
        while (engage.results.size() > 0 && engage.results.size() >= engage.page_size);
    }
}