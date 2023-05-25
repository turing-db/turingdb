
set(db_sources 
            DB.cpp
            Network.cpp
            DBObject.cpp
            DBType.cpp
            DBEntityType.cpp
            DBEntity.cpp
            DBIndex.cpp
            Node.cpp
            Property.cpp
            Edge.cpp

            ValueType.cpp
            Value.cpp

            NodeType.cpp
            EdgeType.cpp
            PropertyType.cpp

            Writeback.cpp

            DBAccessor.cpp
            DBNetworkRange.cpp
            DBNodeTypeRange.cpp
            DBEdgeTypeRange.cpp
            NetworkAccessor.cpp
            NetworkNodeRange.cpp
            NetworkEdgeRange.cpp

            TypeCheck.cpp

            StringIndex.cpp
            SharedString.cpp
            StringRef.cpp)

add_library(turing_db_core_s STATIC ${db_sources})
target_include_directories(turing_db_core_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_core_s
    common_s)
