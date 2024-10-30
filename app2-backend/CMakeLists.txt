set(app2_backend_sources TuringApp2Server.cpp)

add_library(turing_app2_backend_s
    STATIC  ${app2_backend_sources})

# add_custom_target(turing_app2_build
#     ALL
#     COMMAND bash
#             ${CMAKE_CURRENT_SOURCE_DIR}/build_with_git_hash.sh
#             ${CMAKE_CURRENT_BINARY_DIR}/git_hash
#             ${CMAKE_CURRENT_SOURCE_DIR}/../frontend
#             ${CMAKE_CURRENT_SOURCE_DIR}/../frontend_prototype)

target_include_directories(turing_app2_backend_s PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_app2_backend_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(turing_app2_backend_s
    PRIVATE turing_log_s
            common_s
            incbin)

target_compile_definitions(turing_app2_backend_s
    PRIVATE TURING_FRONTEND_PATH="${PROJECT_SOURCE_DIR}/app2-frontend/")
