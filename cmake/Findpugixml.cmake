# Find module wrapper for pugixml that provides pugixml target
# This wraps system-installed pugixml

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_PUGIXML QUIET pugixml)
endif()

find_path(PUGIXML_INCLUDE_DIR
    NAMES pugixml.hpp
    HINTS ${PC_PUGIXML_INCLUDE_DIRS}
    PATH_SUFFIXES pugixml
)

find_library(PUGIXML_LIBRARY
    NAMES pugixml
    HINTS ${PC_PUGIXML_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pugixml
    REQUIRED_VARS PUGIXML_INCLUDE_DIR PUGIXML_LIBRARY
)

if(pugixml_FOUND AND NOT TARGET pugixml)
    add_library(pugixml INTERFACE IMPORTED)
    set_target_properties(pugixml PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${PUGIXML_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "${PUGIXML_LIBRARY}"
    )
endif()
