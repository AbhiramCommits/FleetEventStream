-- FleetEventStream core schema (SQL Server 2022)

CREATE TABLE vehicle_event (
    id              BIGINT IDENTITY (1,1) NOT NULL,
    event_id        NVARCHAR(64)   NOT NULL,
    vehicle_id      NVARCHAR(32)   NOT NULL,
    event_time      DATETIME2(7)   NOT NULL,
    latitude        FLOAT          NOT NULL,
    longitude       FLOAT          NOT NULL,
    speed_kph       FLOAT          NOT NULL,
    heading_deg     FLOAT          NOT NULL,
    alert_code      NVARCHAR(32)   NULL,
    payload         NVARCHAR(MAX)  NOT NULL,
    ingested_at     DATETIME2(7)   NOT NULL CONSTRAINT df_vehicle_event_ingested_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_vehicle_event PRIMARY KEY (id)
);

-- Idempotency is enforced by the database, not by a read-then-write check.
CREATE UNIQUE INDEX ux_vehicle_event_event_id ON vehicle_event (event_id);

-- Supports per-vehicle history lookups ordered newest-first.
CREATE INDEX ix_vehicle_event_vehicle_time ON vehicle_event (vehicle_id, event_time DESC);

-- Supports throughput aggregation over ingested_at.
CREATE INDEX ix_vehicle_event_ingested_at ON vehicle_event (ingested_at);

CREATE TABLE dead_letter_event (
    id              BIGINT IDENTITY (1,1) NOT NULL,
    source_topic    NVARCHAR(255)  NOT NULL,
    partition_id    INT            NOT NULL,
    source_offset   BIGINT         NOT NULL,
    failure_reason  NVARCHAR(1024) NULL,
    error_class     NVARCHAR(255)  NULL,
    raw_payload     NVARCHAR(MAX)  NULL,
    failed_at       DATETIME2(7)   NOT NULL CONSTRAINT df_dead_letter_event_failed_at DEFAULT SYSUTCDATETIME(),
    retry_count     INT            NOT NULL CONSTRAINT df_dead_letter_event_retry_count DEFAULT 0,
    CONSTRAINT pk_dead_letter_event PRIMARY KEY (id)
);

CREATE INDEX ix_dead_letter_event_failed_at ON dead_letter_event (failed_at DESC);
CREATE INDEX ix_dead_letter_event_reason ON dead_letter_event (failure_reason);
