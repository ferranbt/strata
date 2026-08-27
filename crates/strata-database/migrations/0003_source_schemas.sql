-- One row per (mount, path) source endpoint: the canonical schema strata knows for
-- it, as serialized `schema::DataType` JSON (annotations — key, cursor — included).
CREATE TABLE IF NOT EXISTS source_schemas (
    mount       TEXT NOT NULL,
    path        TEXT NOT NULL,
    schema      TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mount, path)
);
