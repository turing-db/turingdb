add_library(turing_db_storage_dump_parquet_s STATIC
    ParquetMetadataParsing.cpp
    PropertyContainerParquetDumper.cpp
    PropertyContainerParquetLoader.cpp
    NodeContainerParquetDumper.cpp
    NodeContainerParquetLoader.cpp
    EdgeContainerParquetDumper.cpp
    EdgeContainerParquetLoader.cpp
    EdgeIndexerParquetDumper.cpp
    EdgeIndexerParquetLoader.cpp
    PropertyIndexerParquetDumper.cpp
    PropertyIndexerParquetLoader.cpp
    StringPropertyIndexerParquetDumper.cpp
    StringPropertyIndexerParquetLoader.cpp
    DataPartParquetDumper.cpp
    DataPartParquetLoader.cpp
    GraphMetadataParquetDumper.cpp
    GraphMetadataParquetLoader.cpp
    CommitJournalParquetDumper.cpp
    CommitJournalParquetLoader.cpp
    TombstonesParquetDumper.cpp
    TombstonesParquetLoader.cpp
    CommitMetaDataParquetDumper.cpp
    CommitMetaDataParquetLoader.cpp
    CommitParquetDumper.cpp
    CommitParquetLoader.cpp
    GraphParquetDumper.cpp
    GraphParquetLoader.cpp)

target_link_libraries(turing_db_storage_dump_parquet_s
    PUBLIC  turing_db_storage_s
            turing_db_io_parquet_s)
