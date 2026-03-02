add_library(greeter_obj OBJECT GreeterExtension.cpp)
target_link_libraries(greeter_obj PRIVATE turing_db_pipeline_s)
target_include_directories(greeter_obj PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
set_property(TARGET greeter_obj PROPERTY POSITION_INDEPENDENT_CODE ON)

add_library(greeter MODULE $<TARGET_OBJECTS:greeter_obj>)
if(APPLE)
    set(EXTENSION_SUFFIX ".dylib")
else()
    set(EXTENSION_SUFFIX ".so")
endif()
set_target_properties(greeter PROPERTIES
    PREFIX ""
    SUFFIX "${EXTENSION_SUFFIX}"
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib/turingdb/extensions)

install(TARGETS greeter DESTINATION lib/turingdb/extensions)
