add_library(turing_db_io_parquet_s STATIC
    ParquetReader.cpp
    ParquetEmbeddingReader.cpp
    ParquetNeo4jVisitor.cpp
    ParquetNodeVisitor.cpp
    ParquetEdgeVisitor.cpp
)

target_include_directories(turing_db_io_parquet_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_io_parquet_s
    PUBLIC  turing_common_s
            turing_db_io_fs_s
            turing_db_storage_s
            Parquet::parquet_static)
