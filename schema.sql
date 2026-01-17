create schema if not exists dw;

create table if not exists dw.dim_station (
    station_eva bigint primary key,
    station_name text not null,
    station_name_search text not null, -- NEW
    lon double precision not null,
    lat double precision not null
);

-- keeps every resolution attempt (top candidate + score)
create table if not exists dw.station_resolve_log (
    id bigserial primary key,
    snapshot_key text not null,
    source_path text not null,
    station_raw text not null,
    station_search text not null,
    best_station_eva bigint,
    best_station_name text,
    best_score double precision,
    auto_linked boolean not null,
    created_at timestamp not null default now()
);

-- only “borderline/failed” cases
create table if not exists dw.needs_review (
    station_search text primary key,     -- de-dupe by normalized/search string
    station_raw text not null,
    best_station_eva bigint,
    best_station_name text,
    best_score double precision,
    last_snapshot_key text not null,
    last_source_path text not null,
    last_seen_at timestamp not null default now()
);

create table if not exists dw.dim_train (
    train_id bigserial primary key,
    category text not null,
    train_number text not null,
    unique (category, train_number)
);

create table if not exists dw.dim_time (
    snapshot_key text primary key,   -- e.g. '2509021400' from folder
    snapshot_ts timestamp not null,  -- parsed timestamp
    snapshot_date date not null,
    hour int not null,
    minute int not null
);

create table if not exists dw.fact_movement (
    movement_key bigserial primary key,

    snapshot_key  text   not null,
    station_eva   bigint not null,
    train_id      bigint not null,

    stop_id text not null,   -- XML <s id="...">

    planned_arrival_ts timestamp,
    planned_departure_ts timestamp,
    planned_arrival_platform text,
    planned_departure_platform text,

    changed_arrival_ts timestamp,
    changed_departure_ts timestamp,

    arrival_cancelled boolean not null default false,
    departure_cancelled boolean not null default false,

    arrival_delay_min integer,
    departure_delay_min integer,

    previous_station_eva bigint,
    next_station_eva bigint,
    
    arrival_is_hidden boolean not null default false,
    departure_is_hidden boolean not null default false,

    unique (snapshot_key, station_eva, stop_id),

    constraint fk_fact_time
        foreign key (snapshot_key) references dw.dim_time(snapshot_key),

    constraint fk_fact_station
        foreign key (station_eva) references dw.dim_station(station_eva),

    constraint fk_fact_train
        foreign key (train_id) references dw.dim_train(train_id),
    
    constraint fk_fact_prev_station
        foreign key (previous_station_eva) references dw.dim_station(station_eva),

    constraint fk_fact_next_station
        foreign key (next_station_eva) references dw.dim_station(station_eva)
);

-- index for fuzzy matching (requires pg_trgm extension)
create index if not exists dim_station_name_trgm_idx
on dw.dim_station using gin (station_name_search gin_trgm_ops);