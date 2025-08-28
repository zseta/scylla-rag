# ScyllaDB RAG vector search example

This example application demonstrates how to build a Retrieval-Augmented Generation (RAG) app using ScyllaDB as the low latency vector database.

The chatbot allows users to ask questions about ScyllaDB, retrieves relevant sections from the official docs, and generates accurate, contextual answers using a local LLM.

## Tech Stack

* ScyllaDB: high-performance NoSQL database for storing vectors and metadata.
* LlamaIndex: chunking mechanism
* Ollama: Model downloads and text embedding


## Components

* Document ingestion: Parses and chunks ScyllaDB documentation into semantically searchable text segments
* Vector embeddings: Converts text into vector representations using an embedding model
* ScyllaDB as Vector Store: Stores embeddings and metadata in ScyllaDB for fast similarity search.

## RAG Pipeline:
1. Retrieve top-k relevant docs from ScyllaDB.
1. Feed them into an LLM prompt alongside the user query.
1. Generate answer.

