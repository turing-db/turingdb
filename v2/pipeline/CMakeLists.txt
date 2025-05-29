
set(pipeline_sources
    PipelineV2.cpp
    Processor.cpp)

add_library(turing_db_v2_pipeline_s STATIC ${pipeline_sources})

target_include_directories(turing_db_v2_pipeline_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_pipeline_s PRIVATE
    turing_db_storage_s)
