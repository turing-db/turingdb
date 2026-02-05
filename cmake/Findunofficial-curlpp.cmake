# Find module wrapper for curlpp that provides unofficial::curlpp::curlpp target
# This wraps system-installed curlpp to match vcpkg naming convention

find_package(PkgConfig REQUIRED)
find_package(CURL REQUIRED)
pkg_check_modules(PC_CURLPP REQUIRED curlpp)

# Get absolute paths for include directories
find_path(CURLPP_INCLUDE_DIR
    NAMES curlpp/cURLpp.hpp
    HINTS ${PC_CURLPP_INCLUDE_DIRS}
)

# Get absolute path for library
find_library(CURLPP_LIBRARY
    NAMES curlpp
    HINTS ${PC_CURLPP_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(unofficial-curlpp
    REQUIRED_VARS CURLPP_INCLUDE_DIR CURLPP_LIBRARY
)

if(unofficial-curlpp_FOUND AND NOT TARGET unofficial::curlpp::curlpp)
    add_library(unofficial::curlpp::curlpp INTERFACE IMPORTED)
    set_target_properties(unofficial::curlpp::curlpp PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${CURLPP_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "${CURLPP_LIBRARY};CURL::libcurl"
    )
endif()
