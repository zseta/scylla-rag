from typing import List
import uuid
from scylladb import ScyllaClient
from llama_index.core.node_parser import MarkdownNodeParser
import ollama
from llama_index.core.node_parser import (
    SemanticDoubleMergingSplitterNodeParser,
    LanguageConfig,
)
from llama_index.core import SimpleDirectoryReader
from llama_index.core import Document
from llama_index.core.schema import BaseNode

class ScyllaRag():
    
    EMBEDDING_MODEL = "hf.co/CompendiumLabs/bge-base-en-v1.5-gguf"
    LANGUAGE_MODEL = "hf.co/bartowski/Llama-3.2-1B-Instruct-GGUF"
    
    def __init__(self):
        self.lang_config = LanguageConfig(spacy_model="en_core_web_md")
        
    def create_embedding(self, content):
        return ollama.embed(model=self.EMBEDDING_MODEL, input=content)["embeddings"][0]
        
    def create_chunks(self, dir_path: str, files_limit=1) -> List[BaseNode]:
        documents = SimpleDirectoryReader(input_dir=dir_path,
                                          recursive=True,
                                          num_files_limit=files_limit,
                                          required_exts=[".md", ".rst"],
                                          exclude_empty=True,
                                          exclude_hidden=True).load_data()
        
        splitter = SemanticDoubleMergingSplitterNodeParser(
            language_config=self.lang_config,
            initial_threshold=0.4, # merge sentences to create chunks
            appending_threshold=0.5, # merge chunk to the following sentence
            merging_threshold=0.5, # merge chunks to create bigger chunks
            max_chunk_size=2048,    
        )
        return splitter.get_nodes_from_documents(documents, show_progress=True)
    
    def create_chunks_md(self, dir_path: str, files_limit=1) -> List[BaseNode]:
        markdown_docs = SimpleDirectoryReader(input_dir=dir_path,
                                          recursive=True,
                                          num_files_limit=files_limit,
                                          required_exts=[".md"],
                                          exclude_empty=True,
                                          exclude_hidden=True).load_data()
        parser = MarkdownNodeParser()
        return parser.get_nodes_from_documents(markdown_docs, show_progress=True)
    
    def vectorize(self, nodes: List[BaseNode], target_table: str) -> list[Document]:
        db_client = ScyllaClient()
        for node in nodes:
            chunk_id = uuid.uuid4()
            text = node.get_content()
            embedding = scylla_rag.create_embedding(text)
            db_client.insert_data(target_table, {"text": text,
                                                "chunk_id": chunk_id,
                                                "embedding": embedding})

    def fetch_chunks(self, table: str, user_query: str, top_k=5) -> List[Document]:
        db_client = ScyllaClient()
        user_query_embedding = self.create_embedding(user_query)
        db_query = f"""SELECT text, 
                    similarity_cosine(embedding, %s) as distance 
                    FROM {table} 
                    ORDER BY embedding ANN OF %s LIMIT %s;
                   """
        values = [user_query_embedding, user_query_embedding, top_k]
        return db_client.query_data(db_query, values)
    
    def query_llm(self, user_query: str, chunks: list[str]) -> str:
        context_prompt = ""
        for i, chunk in enumerate(chunks):
            context_prompt += f"\n\n Item {i+1}: {chunk}"
        system_prompt = f"""You are a helpful AI assistant chatbot. \n        
        Context:
        {context_prompt} \n
        
        Use the provided context as knowledge base to form the best answer you can. \n
        
        """
        print("System prompt:", system_prompt)
        stream = ollama.chat(
            model=self.LANGUAGE_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query},
            ],
            stream=True,
        )
        print("Chatbot response:")
        for chunk in stream:
            print(chunk["message"]["content"], end="", flush=True)
    
if __name__ == "__main__":
    scylla_rag = ScyllaRag()
    
    # Chunking and vectorization (one time operation)
    #nodes = scylla_rag.create_chunks("../scylladb/docs", files_limit=1)
    #nodes = scylla_rag.create_chunks_md("../scylladb/docs", files_limit=None)
    #scylla_rag.vectorize(nodes, target_table="rag.md_chunks")
    
    
    user_input = input("Enter your question: ")
    nodes = scylla_rag.fetch_chunks("rag.md_chunks", user_input, top_k=3)
    
    lines = [f"{node['text']}\n\n---\n\n " for node in nodes]
    with open("retrieved_nodes.md", "w", encoding="utf-8") as f:
        f.writelines(lines)
    
    scylla_rag.query_llm(user_input, [node["text"] for node in nodes])