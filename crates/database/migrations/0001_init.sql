-- Catalog registry snapshot: one row per mounted provider instance. `mount` is the
-- router location / config section name (unique — strata's Registry is keyed by
-- mount), `backend` is the provider type that answers it. Persisting this snapshot
-- gives datasets, sync cursors, and lineage a stable mount identity to reference.
-- This is the seed of the "Catalog DB" keystone (issues.md #1); more tables follow.
CREATE TABLE IF NOT EXISTS mounts (
    mount       TEXT PRIMARY KEY,
    backend     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
