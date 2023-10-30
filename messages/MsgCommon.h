#ifndef _BIO_MSG_COMMON_
#define _BIO_MSG_COMMON_

#include "Message.h"

namespace msg {

MSG_ERROR(ERROR_FAILED_TO_CREATE_DIRECTORY, 1001,
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
          "Impossible to remove the directory '$0'.")

MSG_ERROR(ERROR_FAILED_TO_OPEN_FOR_WRITE, 1007,
          "Failed to open file '$0' for write.")

MSG_FATAL(FATAL_NO_TCL_INTERP, 1008,
          "Failed to create Tcl interpreter.")

MSG_ERROR(ERROR_NOT_A_DIRECTORY, 1009,
          "The file '$0' was expected to be a directory.")

MSG_ERROR(ERROR_FAILED_TO_REMOVE_FILE, 1010,
          "Impossible to remove the file '$0'.")

MSG_ERROR(ERROR_EXECUTABLE_NOT_FOUND, 1011,
          "Impossible to find executable '$0'.")

MSG_ERROR(ERROR_FAILED_TO_WRITE_COMMAND_SCRIPT, 1012,
          "Can not write command script '$0'.")

MSG_ERROR(ERROR_FAILED_TO_WRITE_FILE, 1013,
          "Writing file '$0' failed.")

MSG_ERROR(ERROR_FILE_NOT_EXISTS, 1014, 
          "The file '$0' does not exist.")

MSG_ERROR(ERROR_INCORRECT_CMD_USAGE, 1015,
          "The argument '$0' was not used correctly.")

MSG_ERROR(ERROR_INCORRECT_ENV_SETUP, 1016,
          "The TURING_HOME env variable is not set")

MSG_ERROR(ERROR_IMPOSSIBLE_TO_RUN_COMMAND, 1017,
          "Impossible to launch a command: $0")

MSG_ERROR(ERROR_FAILED_TO_RUN_SCRIPT, 1018,
          "Impossible to run the script $0")

}

#endif
