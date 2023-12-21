
set(db_sources 
            LabelIDMap.cpp
            Storage.cpp
            StorageAccessor.cpp
            Node.cpp
            DB.cpp)

add_library(turing_db_core_s STATIC ${db_sources})
target_include_directories(turing_db_core_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_core_s
    common_s)
