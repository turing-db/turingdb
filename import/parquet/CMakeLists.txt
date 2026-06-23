add_library(turing_db_import_parquet_s STATIC  
    Neo4jParquetImporter.cpp
)

target_link_libraries(turing_db_import_parquet_s
    PUBLIC  turing_common_s
            turing_db_jobs_s
            turing_db_storage_s
            turing_db_import_common_s
            turing_db_io_parquet_s
    PRIVATE 
            range-v3)

target_include_directories(turing_db_import_parquet_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
