
set(db_sources 
            DB.cpp
            Network.cpp
            Node.cpp
            NodeDescriptor.cpp

            ValueType.cpp
            Value.cpp

            NodeType.cpp

            Writeback.cpp

            StringIndex.cpp
            SharedString.cpp
            StringRef.cpp)

add_library(turing_db_s STATIC ${db_sources})
target_include_directories(turing_db_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_s PRIVATE ${BIO_SOURCE_DIR}/messages)
target_link_libraries(turing_db_s
    common_s)
