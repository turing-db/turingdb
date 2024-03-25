#ifndef _BIO_MSG_RUN_
#define _BIO_MSG_RUN_

#include "Message.h"

namespace msg {

MSG_ERROR(ERROR_FAILED_TO_RUN_NOTEBOOK, 4001,
          "Failed to run notebook '$0'.")

MSG_ERROR(ERROR_FAILED_TO_CONVERT_NOTEBOOK, 4002,
          "Failed to convert notebook '$0' to $1 format.")

MSG_INFO(INFO_RUNNING_NOTEBOOK, 4003,
         "Running notebook $0.")

MSG_INFO(INFO_CONVERTING_NOTEBOOK, 4004,
         "Converting notebook $0 to $1")

MSG_INFO(INFO_JUPYTER_LOG_FILE, 4005,
         "The notebook log file is $0")

MSG_INFO(INFO_NOTEBOOK_REPORT, 4006,
         "Notebook report written in $0")
    
MSG_ERROR(ERROR_FAILED_TO_GENERATE_REPORT, 4007,
          "Failed to generate report for notebook '$0'")

MSG_ERROR(ERROR_FAILED_TO_PARSE_NB_ARG, 4008,
          "Failed to parse -nbarg ('$0'). The format must be ENV_NAME=ENV_VALUE")

}

#endif
