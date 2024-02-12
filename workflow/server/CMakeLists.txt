
set(server_sources
            WorkflowServerHandler.cpp
            WorkflowServerConfig.cpp
            WorkflowServer.cpp)

add_library(turing_workflow_server_s STATIC ${server_sources})

target_include_directories(turing_workflow_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_workflow_server_s PRIVATE
    common_s
    turing_rest_s)
