# Pipeline

## Initial Setup

Extract the content you unzip from the folders `/DBahn-berlin/timetable` and `/DBahn-berlin/timetable_changes` respectively into folders named `/timetable` and `/timetable_changes`. So the folder structure looks like the following:

```
.
├── DBahn-berlin
│   ├── ...
│   ├── timetable_changes
│   └── timetables
├── ...
├── timetable_changes
└── timetables
```

The folders `/timetable` and `/timetable_changes` will be gitignored.

## The Data

You start from three separate raw components (Stations `.json`, Timetables `.xml`, Timetable Changes `.xml`) and design your own star schema that can represent all of them once ingested.

### Station:

Must-haves:

- `name`: station name
- `evaNumbers`: to tie with the `XML` files. (`XML` should be using `evaNumbers.number`)
    - Use EVA numbers for `XML` joins.

Nice-to-haves:

- `number`: DB station record id, but the XML is EVA-based, so not our primary join key.
- `ifopt`: useful as an additional stable identifier and for debugging

This is what we would have with the two must-have params:

```json
{
      "name": "Ahrensfelde",
      "evaNumbers": [
        {
          "number": 8011003,
          "geographicCoordinates": {
            "type": "Point",
            "coordinates": [
              13.565154,
              52.571375
            ]
          },
          "isMain": true
        },
        {
          "number": 8089188,
          "geographicCoordinates": {
            "type": "Point",
            "coordinates": [
              13.565551,
              52.5712445
            ]
          },
          "isMain": false
        }
      ],
}
```

> [!IMPORTANT]
> `"isMain": true` means this is the primary one among several identifiers for the same station record. Only consider entries with `true`.

### Timetable

Folder structure:

```
.
└── timetables
    └── week
	    └── hour
		    └── station_timetable

```

Example `berlin_ostbahnhof_timetable.xml` from path `/timetables/250902_250909/2509021400` (On day 02.09.25 at 14:00):

How to read an `<s>` block like a timetable entry:

The root:

```xml
<timetable station="Berlin Ostbahnhof">
```

Each `<s>` is one stop (one train calling at this station).
Example:

```xml
<s id="5871316147024433626-2509021411-3">
  <tl f="N" t="p" o="800165" c="RB" n="56935" />
  <ar pt="2509021420" pp="1" l="23" ppth="Berlin Friedrichstraße|Berlin Alexanderplatz" />
  <dp pt="2509021421" pp="1" l="23" ppth="Berlin Ostkreuz|Flughafen BER" />
</s>
```

- Stop ID: `5871316147024433626-2509021411-3`
	- Unique identifier for "this trip at this station (and this stop index)"
- Train label: `<tl .../>`
	- `f="N"`: flags (can be ignored)
	- `t="p"`: trip type (here _planned_)
	- `o="800165"` operator/owner code
	- `c="RB"`: category (here _RB_)
	- `n="56935"`: train number
	- So train **RB 56935**
- Arrival event `<ar .../>`
	- `pt="2509021420"`: planned arrival time = `2025-09-02 14:20`
	- `pp="1"`: planned platform = `1`
	- `l="23"`: line indicator (often relevant for S/RB)
	- `ppth="Berlin Friedrichstraße|Berlin Alexanderplatz"`
		- Stations before the current station, so it came from `Friedrichstrasse -> Alexanderplatz -> Ostbahnhof`
- Departure event `<dp .../>`
	- `pt="2509021421"`: planned departure time = `2025-09-02 14:21`
	- `pp="1"`: planned platform
	- `l="23"`: line indicator
	- `ppth="Berlin Ostkreuz|Flughafen BER"`
		- Stations after the current station, so it goes `Ostbahnhof -> Ostkreuz -> Flughafen BER`

**Safe to ignore:**
- `tl @f` (flags)
- `tl @t` (trip type)
- `tl @o` (operator/owner)
- `ar/dp @l` (line indicator)
- `ar/dp @ppth` (path string)
	- For the last tasks (graph) we'll need "connections/edges", but we can derive edges from other sources (e.g. stop sequences via trip IDs)

**Keep:**

- `s @id` (stop ID)
    - Best stable key to de-duplicate and join planned vs changes for same stop/event
- `tl @c` (category) and `tl @n` (train number)
	- Concatenate them to get the train name.
	- These could be ignored and the delays/cancellations could still be computed. But they are tiny and useful for debugging ("which train is this?"), so keep.
- `ar/dp @pt` (planned time)
	- Must have: Needed for delay computations (changed - planned)
- `ar/dp @pp` (planned platform)
	- Not required for core tasks, but cheap to keep and helps sanity-checking.


### Timetable Change

Folder structure:

```
.
└── timetables_changes
    └── week
	    └── 15_mins
		    └── station_change
```

Example `berlin_hauptbahnhof_change.xml` from path `/timetable_changes/250902_250909/2509021600` (On day 02.09.25 at 16:00):

The root:

```xml
<timetable station="Berlin Hbf" eva="8011160">
```

Each `<s>` is one stop (one train calling at this station).
Example:

```xml
<s id="-4020550040361167307-2509021345-8" eva="8011160">
    <m id="r2415041" t="h" from="2505120800" to="2509202359" cat="Information" ts="2505112304" ts-tts="25-09-02 10:48:50.464" pr="3" />
    <ar ct="2509021817">
      <m id="r23682215" t="d" c="43" ts="2509021516" ts-tts="25-09-02 15:16:53.743" />
    </ar>
    <dp ct="2509021821">
      <m id="r23682215" t="d" c="43" ts="2509021516" ts-tts="25-09-02 15:16:53.743" />
    </dp>
</s>
```

- Label `<s .../>`
	- Stop ID: `"5871316147024433626-2509021411-3"`
		- This is your best key to match the change record to the corresponding planned stop (so you can compare `ct` vs `pt`).
	- EVA: `"8011160"`
		- station join key back to `dim_station` and the planned timetable.
- Label `<m .../>`
	- `id="r2415041"`
	- `t="h"`
	- `from="2505120800"`
	- `to="2509202359"`
	- `cat="Information"`
	- `ts="2505112304"`
	- `ts-tts="25-09-02 10:48:50.464"`
	- `pr="3"`
- Arrival event `<ar .../>`
	- `ct="2509021817"`: changed time
	- Label `<m .../>`
		- `id="r23682215"`
		- `t="d"`
		- `c="43"`
		- `ts="2509021516"`
		- `ts-tts="25-09-02 15:16:53.743"`
			- ignore and rely on the compact `ts` or the snapshot folder time
- Departure event `<dp .../>`
	- `ct="2509021821"`: changed time
	- Label `<m .../>`
		- `id="r23682215"`
		- `t="d"`
		- `c="43"`
		- `ts="2509021516"`
		- `ts-tts="25-09-02 15:16:53.743"`

**Safe to ignore:**
- Station-level `<m .../>` directly under `<s>`
 - Event-level `<m .../>` inside `<ar>` / `<dp>`
 - `ts-tts="25-09-02 ..."`
	- This is a human-friendly timestamp string. You can ignore it and rely on the compact `ts` or your snapshot folder time.

**Keep:**
- Root `eva="8011160"` (or the stop `eva`)  
    This is your station join key back to `dim_station` and the planned timetable.
- Stop `id="…"`  
    This is your best key to match the change record to the corresponding planned stop (so you can compare `ct` vs `pt`).
- Event changed time
    - `ar @ct` and/or `dp @ct`  
        Needed to compute delay: `delay_minutes = ct - pt`.
- Event cancellation status if present
    - In many change files you’ll also see `cs="c"` (cancelled) on `ar`/`dp` (or a related cancellation indicator like `clt`).  
        That’s what you’ll use for counting cancellations per snapshot.  
        _(This example doesn’t show `cs`, but it will appear in other records.)_
- Snapshot timestamp
    - Not in the XML fields you listed, but you must take it from the folder name (e.g., `2509021600`) because tasks ask "at a time snapshot (date hour)".