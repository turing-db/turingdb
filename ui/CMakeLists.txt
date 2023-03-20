
# Next.js UI site
file(GLOB_RECURSE site_files site/package.json site/package-lock.json)

add_custom_command(OUTPUT site.tar SiteArchiveData.h
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/generate_archive.sh ${CMAKE_CURRENT_SOURCE_DIR}/site ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ${site_files})
add_custom_target(turing_ui_site DEPENDS site.tar SiteArchiveData.h)

# UI web server library
set(ui_server_sources
    SiteArchive.cpp
    TuringUIServer.cpp)

add_library(turing_ui_server_s STATIC ${ui_server_sources})
add_dependencies(turing_ui_server_s turing_ui_site)

target_include_directories(turing_ui_server_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(turing_ui_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
target_include_directories(turing_ui_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_ui_server_s PRIVATE
    turing_log_s
    common_s
    incbin)
