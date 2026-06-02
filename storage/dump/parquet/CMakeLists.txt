add_library(turing_db_storage_dump_parquet_s STATIC
    PropertyContainerParquetDumper.cpp
    PropertyContainerParquetLoader.cpp
    NodeContainerParquetDumper.cpp
    NodeContainerParquetLoader.cpp
    EdgeContainerParquetDumper.cpp
    EdgeContainerParquetLoader.cpp
    EdgeIndexerParquetDumper.cpp
    EdgeIndexerParquetLoader.cpp)

target_link_libraries(turing_db_storage_dump_parquet_s
    PUBLIC  turing_db_storage_s
            turing_db_io_parquet_s)
