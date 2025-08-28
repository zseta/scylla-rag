CREATE KEYSPACE rag WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

CREATE TABLE rag.chunks (
    chunk_id UUID PRIMARY KEY,
    text TEXT,
    embedding vector<float, 768>
);


CREATE TABLE rag.md_chunks (
    chunk_id UUID PRIMARY KEY,
    text TEXT,
    embedding vector<float, 768>
);

CREATE INDEX IF NOT EXISTS md_ann_index ON rag.md_chunks(embedding) USING 'vector_index';