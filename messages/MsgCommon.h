#ifndef _BIO_MSG_COMMON_
#define _BIO_MSG_COMMON_

#include "Message.h"

namespace msg {

MSG_FATAL(FATAL_FAILED_TO_CREATE_DIRECTORY, 1001,
    "Failed to create directory '$0'")

MSG_ERROR(ERROR_DIRECTORY_NOT_EXISTS, 1002,
    "The directory '$0' does not exist")

MSG_ERROR(ERROR_FAILED_TO_COPY, 1003,
    "Failed to copy '$0' to '$1'")

MSG_ERROR(ERROR_FAILED_TO_OPEN_FOR_READ, 1004,
    "Failed to open file '$0' for read.")

MSG_INFO(INFO_READING_FILE, 1005,
    "Reading file '$0'")

MSG_ERROR(ERROR_FAILED_TO_REMOVE_DIRECTORY, 1006,
    "Failed to remove the directory '$0'.")

MSG_ERROR(ERROR_FAILED_TO_OPEN_FOR_WRITE, 1007,
    "Failed to open file '$0' for write.")

MSG_FATAL(FATAL_NO_TCL_INTERP, 1008,
    "Failed to create Tcl interpreter.")

}

#endif
