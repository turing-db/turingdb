set(system_sources
    TuringConfig.cpp
    GraphLoadStatus.cpp
    LockFile.cpp
    LockFileResult.cpp
    SystemEventHandler.cpp
    GraphManager.cpp
    SystemManager.cpp
    SystemAccessor.cpp)

add_library(turing_db_system_s STATIC ${system_sources})
target_include_directories(turing_db_system_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_system_s PUBLIC
    turing_db_storage_s
    turing_db_procedures_s
    turing_db_io_fs_s
    turing_db_jobs_s
    turing_db_import_gml_s
    turing_db_import_jsonl_s
    turing_db_import_common_s
    turing_db_io_s3_s
    turing_vector_s
    turing_extensions_s
    turing_common_s
)
