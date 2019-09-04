def chunk_list(list_to_chunk, chunk_size):
    """Yield successive chunks from a list."""
    for idx in range(0, len(list_to_chunk), chunk_size):
        yield list_to_chunk[idx:idx + chunk_size]
