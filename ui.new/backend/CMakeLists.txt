set(uinew_server_sources
            SiteArchive.cpp
            ServerThread.cpp
            BioserverThread.cpp
            FlaskThread.cpp
            ReactThread.cpp
            TailwindThread.cpp
            ServerThreadEngine.cpp
            TuringUIServer.cpp)

add_library(turing_uinew_server_s
    STATIC  ${uinew_server_sources})

file(GLOB_RECURSE site_files
            site/package.json
            site/package-lock.json)

add_custom_command(
    OUTPUT  site.tar
            SiteArchiveData.h
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/../generate_archive.sh
              ${CMAKE_CURRENT_SOURCE_DIR}/../frontend
              ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ${site_files}
            ${CMAKE_CURRENT_SOURCE_DIR}/../generate_archive.sh)

add_custom_target(turing_uinew_site
    DEPENDS site.tar
            SiteArchiveData.h)

add_dependencies(turing_uinew_server_s
            turing_uinew_site)

target_include_directories(turing_uinew_server_s
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
            ${BIO_SOURCE_DIR}/messages
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_uinew_server_s
    PRIVATE turing_log_s
            common_s
            incbin)

if(${TURING_DEV})

target_compile_definitions(turing_uinew_server_s
    PRIVATE TURINGUI_BACKEND_SRC_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../")

endif()

install(
    DIRECTORY   turingui
    DESTINATION lib/python
    PATTERN     *.swp
    EXCLUDE)
