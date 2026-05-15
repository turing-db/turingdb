
set(turing_parquet_sources
    TuringParquetTool.cpp
    ParquetSchema.cpp
    ParquetSchemaExtractor.cpp
    ParquetJsonDetector.cpp
    ParquetPropertyAnalysis.cpp
    ParquetPropertyAnalyzer.cpp
    ParquetGraphMapping.cpp)

add_executable(turing-parquet ${turing_parquet_sources})
target_link_libraries(turing-parquet
    PUBLIC  turing_db_io_parquet_s
            argparse
            tabulate
    PRIVATE nlohmann_json)

install(TARGETS turing-parquet DESTINATION bin)
