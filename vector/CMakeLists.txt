set(VECTOR_SOURCES
        RandomGenerator.cpp
        VectorException.cpp
        VectorStorageManager.cpp
        LSHShardRouter.cpp
        VecLibShardAccessor.cpp
        VectorResult.cpp
        VectorDatabase.cpp
        VecLibMetadataLoader.cpp
        VecLibMetadataWriter.cpp
        LSHShardRouterLoader.cpp
        LSHShardRouterWriter.cpp
        ShardCache.cpp
        VecLibShard.cpp
        VecLibAccessor.cpp
        VecLibWriteAccessor.cpp
        VecLib.cpp
        VectorCSVReader.cpp
        import/ImportResult.cpp
        import/ImportException.cpp
        import/JsonImporter.cpp)

add_library(turing_vector_s STATIC ${VECTOR_SOURCES})

target_include_directories(turing_vector_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_vector_s
    PUBLIC  turing_common_s
            Threads::Threads
            turing_db_io_fs_s
            turing_db_io_cache_s
            nlohmann_json::nlohmann_json
            faiss
            OpenMP::OpenMP_CXX)
