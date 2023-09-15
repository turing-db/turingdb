function(add_cython_library)
    set(options "")
    set(one_value_args LIBRARY PKG_ROOT SOURCES)
    set(multi_value_args "")
    cmake_parse_arguments(ADD_CYTHON_LIB ${options} ${one_value_args} ${multi_value_args} ${ARGN})

    foreach(py_file ${ADD_CYTHON_LIB_SOURCES})
        # Get path of the python file relative to the package root dir
        file(RELATIVE_PATH path_in_pkg ${ADD_CYTHON_LIB_PKG_ROOT} ${py_file})

        # Compute generated C file path
        set(path_in_out_pkg ${CMAKE_CURRENT_BINARY_DIR}/${path_in_pkg})
        string(REGEX REPLACE "[.]py$" ".c" generated_file ${path_in_out_pkg})

        set_source_files_properties(${generated_file} PROPERTIES GENERATED TRUE)

        # Get path of the module directory to create in build dir
        get_filename_component(dir_in_out_pkg ${path_in_out_pkg} DIRECTORY)

        # Generate C file
        add_custom_command(
            OUTPUT ${generated_file}
            DEPENDS ${py_file}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${dir_in_out_pkg}
            COMMAND cython3 -3 ${py_file} --output-file ${generated_file})

        # Compute library target name
        string(REGEX REPLACE "/" "_" target_name ${path_in_pkg})
        string(REGEX REPLACE "[.]py$" "" target_name ${target_name})
        set(target_name "cython_${target_name}")

        # Shared library target
        add_library(${target_name} SHARED ${generated_file})
        target_include_directories(${target_name} PRIVATE ${Python3_INCLUDE_DIRS})
        set_target_properties(${target_name} PROPERTIES 
            LIBRARY_OUTPUT_DIRECTORY ${dir_in_out_pkg})

        # Compute library file name
        get_filename_component(lib_file_name ${generated_file} NAME_WE)

        # Clean up .so file name and prefix
        set_target_properties(${target_name} PROPERTIES
            PREFIX ""
            OUTPUT_NAME ${lib_file_name})

        get_filename_component(dir_name_in_pkg ${path_in_pkg} DIRECTORY)
        install(TARGETS ${target_name} DESTINATION lib/python/${dir_name_in_pkg})

    endforeach()

endfunction()
