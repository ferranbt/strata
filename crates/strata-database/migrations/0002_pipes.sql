-- One row per configured pipe: a durable source→destination sync. `cursor` is the
-- source's last resume token (the `next` of its page cursor) so a restart continues
-- where it left off. Mirrors `strata_types::Pipe` (source/destination flattened to
-- mount+path columns).
CREATE TABLE IF NOT EXISTS pipes (
    id                  UUID NOT NULL UNIQUE,
    source_mount        TEXT NOT NULL,
    source_path         TEXT NOT NULL,
    destination_mount   TEXT NOT NULL,
    destination_path    TEXT NOT NULL,
    cursor              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_mount, source_path, destination_mount, destination_path)
);
