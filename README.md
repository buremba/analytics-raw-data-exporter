[![Build Status](https://travis-ci.org/buremba/rakam-data-importer.svg?branch=master)](https://travis-ci.org/buremba/rakam-data-importer)

# rakam-data-importer
Import data from external analytics services to Rakam

Download latest version from [releases](//github.com/buremba/rakam-data-importer/releases) and run the following command in order to see the supported services and operations of each service:

```bash
java -jar data-importer-*-jar-with-dependencies.jar 
```

# Importing data from Mixpanel

You can import event data directly from Mixpanel and let the application handle column transformation by fetching event metadata and 
creating appropriate fields in Rakam.
Mixpanel uses `$` prefix for internal fields, Rakam will automatically convert it to `_` and send event data to Rakam in batches.

If you want to map Mixpanel fields to your custom fields in Rakam, you can export event metadata with `mixpanel explain-events`.
It will output the Mixpanel event schema and map them to appropriate Rakam fields. You can modify the mapping file and 
use it when importing data with `mixpanel import-events` using `--schema` or `--schema-file` arguments.

Importing people data works the same way, Rakam can handle the mappings if you don't specify one of the `--schema` or `--schema-file` arguments
but you can also use your custom mapping by exporing people metadata with `mixpanel explain-people` and import people data with `mixpanel import-people`

Here are the basic commands that automatically fetches all event and people data from Mixpanel and import them to your Rakam cluster.

```bash
java -jar data-importer-*-jar-with-dependencies.jar mixpanel import-events
    --rakam.address [RAKAM_CLUSTER_ADDRESS]
    --rakam.project [RAKAM_PROJECT]
    --mixpanel.api-secret [MIXPANEL_API_SECRET]
    --mixpanel.api-key [MIXPANEL_API_KEY]
    --rakam.project.write-key [RAKAM_WRITE_KEY]
```

```bash
java -jar data-importer-*-jar-with-dependencies.jar mixpanel import-people
    --rakam.address [RAKAM_CLUSTER_ADDRESS]
    --rakam.project [RAKAM_PROJECT]
    --mixpanel.api-secret [MIXPANEL_API_SECRET]
    --mixpanel.api-key [MIXPANEL_API_KEY]
    --rakam.project.write-key [RAKAM_WRITE_KEY]
```

Available commands:

```bash
$ java -jar data-importer-*-jar-with-dependencies.jar help mixpanel

NAME
        [app] mixpanel -

SYNOPSIS
        [app] mixpanel
        [app] mixpanel explain-events
                --mixpanel.api-secret <apiSecret>
                --mixpanel.api-key <apiKey>
        [app] mixpanel explain-people 
                --mixpanel.api-secret <apiSecret>
                --mixpanel.api-key <apiKey>
        [app] mixpanel import-events 
                --rakam.address <rakamAddress>
                [--start <startDate>]
                [--end <endDate>]
                [--schema.file <schemaFile>]
                --mixpanel.api-secret <apiSecret>
                [--duration <duration>]
                [--schema <schema>] 
                --mixpanel.api-key <apiKey>
                --rakam.project <project>
                [--mixpanel.project.timezone <projectTimezone>]
                --rakam.project.write-key <rakamWriteKey>
        [app] mixpanel import-people 
                --rakam.address <rakamAddress>
                --rakam.project <rakamProject>
                [--schema <schema>]
                [--schema.file <schemaFile>] 
                [--last-seen <lastSeen>]
                --mixpanel.api-secret <apiSecret>
                --mixpanel.project.timezone <projectTimezone>
                --mixpanel.api-key <apiKey> 
                --rakam.project.write-key <rakamWriteKey>

COMMANDS
        With no arguments, Display help information

        import-events
            Mixpanel importer

            With --rakam.address option, Rakam cluster url

            With --start option, Mixpanel event start date

            With --end option, Mixpanel event end date

            With --schema.file option, Mixpanel event schema file

            With --mixpanel.api-secret option, Api secret

            With --duration option, Mixpanel event import duration

            With --schema option, Mixpanel event schema file

            With --mixpanel.api-key option, Api key

            With --rakam.project option, Project

            With --mixpanel.project.timezone option, Mixpanel project utc.

            With --rakam.project.write-key option, Project

        explain-events
            Mixpanel importer

            With --mixpanel.api-secret option, Api secret

            With --mixpanel.api-key option, Api key

        import-people
            Mixpanel importer

            With --rakam.address option, Rakam cluster url

            With --rakam.project option, Rakam cluster url

            With --schema option, Mixpanel people schema

            With --schema.file option, Mixpanel people schema file

            With --last-seen option, Mixpanel people lastSeen filter as date
            (YYYY-mm-dd)

            With --mixpanel.api-secret option, Api secret

            With --mixpanel.project.timezone option, Rakam cluster url

            With --mixpanel.api-key option, Api key

            With --rakam.project.write-key option, Project

        explain-people
            Mixpanel people schema explainer

            With --mixpanel.api-secret option, Api secret

            With --mixpanel.api-key option, Api key
```
