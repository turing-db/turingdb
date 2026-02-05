# Find module wrapper for inih that provides unofficial::inih::inireader target
# This wraps system-installed inih to match vcpkg naming convention

find_path(INIH_INCLUDE_DIR
    NAMES INIReader.h
    PATH_SUFFIXES INIReader
)

find_library(INIREADER_LIBRARY
    NAMES INIReader
)

find_library(INIH_LIBRARY
    NAMES inih
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(unofficial-inih
    REQUIRED_VARS INIH_INCLUDE_DIR INIREADER_LIBRARY INIH_LIBRARY
)

if(unofficial-inih_FOUND AND NOT TARGET unofficial::inih::inireader)
    add_library(unofficial::inih::inireader INTERFACE IMPORTED)
    set_target_properties(unofficial::inih::inireader PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${INIH_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "${INIREADER_LIBRARY};${INIH_LIBRARY}"
    )
endif()
