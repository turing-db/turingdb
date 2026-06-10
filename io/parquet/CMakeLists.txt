add_library(turing_db_io_parquet_s STATIC
    ParquetReader.cpp
    ParquetWriter.cpp
    ParquetWriteSchema.cpp)

target_include_directories(turing_db_io_parquet_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_io_parquet_s
    PUBLIC  turing_common_s
            turing_db_io_fs_s
            Parquet::parquet_static)
