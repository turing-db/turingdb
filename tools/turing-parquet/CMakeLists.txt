
set(turing_parquet_sources
    TuringParquetTool.cpp
    ParquetSchema.cpp
    ParquetSchemaExtractor.cpp
    ParquetJsonDetector.cpp
    ParquetPropertyAnalysis.cpp
    ParquetPropertyMerge.cpp
    ParquetPropertyAnalyzer.cpp
    ParquetEdgeTypeAnalysis.cpp
    ParquetEdgeTypeAnalyzer.cpp
    ParquetGraphMapping.cpp
    ParquetGraphImporter.cpp
    ParquetNodeRowVisitor.cpp
    ParquetEdgeRowVisitor.cpp)

add_executable(turing-parquet ${turing_parquet_sources})
target_link_libraries(turing-parquet
    PUBLIC  turing_db_io_parquet_s
            turing_db_storage_s
            turing_db_system_s
            turing_db_s
            argparse
            tabulate
    PRIVATE nlohmann_json)

install(TARGETS turing-parquet DESTINATION bin)
