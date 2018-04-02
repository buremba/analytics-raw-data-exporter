package org.rakam.importer.amplitude;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AmplitudeEvent
{
    public String device_carrier;
    public String city;
    public String user_id;
    public String uuid;
    public String event_time;
    public String platform;
    public String os_version;
    public String user_creation_time;
    public String ip_address;
    public boolean is_attribution_event;
    public String dma;
    public Map<String, Object> group_properties;
    public Map<String, Object> user_properties;
    public String client_upload_time;
    public String event_type;
    public String library;
    public String device_type;
    public String device_manufacturer;
    public float location_lng;
    public float location_lat;
    public String os_name;
    public String amplitude_event_type;
    public String device_brand;
    public Map<String, Object> groups;
    public Map<String, Object> event_properties;
    public Map<String, Object> data;
    public String device_id;
    public String language;
    public String device_model;
    public String country;
    public String region;
    public String adid;
    public long session_id;
    public String device_family;
    public String idfa;
    public Double revenue;
}
