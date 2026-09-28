CREATE KEYSPACE IF NOT EXISTS ferry_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS ferry_demo.events (
    tenant_id text,
    event_id  int,
    status    text,
    payload   text,
    PRIMARY KEY ((tenant_id), event_id)
);
