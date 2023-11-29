#include "Message.h"

namespace msg {

MSG_ERROR(ERROR_LLM_MODEL_BUILDER_FAILED, 9001,
          "The generation of the model API failed")

MSG_INFO(INFO_LLM_MODEL_API_GENERATION_COMPLETE, 9002,
         "The generation of the model API has been successful. The generated code has been written in $0")

MSG_ERROR(ERROR_LLM_BAD_PYTHON_NAME_FOR_MODULE, 9003,
          "The name '$0' is not supported for a Python module.")

MSG_ERROR(ERROR_LLM_BAD_PYTHON_NAME_FOR_CLASS, 9004,
          "The name '$0' is not supported for a Python class.")

MSG_ERROR(ERROR_LLM_SERVER_SCRIPT_GENERATION_FAILED, 9005,
          "The generation of the server script failed.")

MSG_ERROR(ERROR_LLM_RUN_SCRIPT_GENERATION_FAILED, 9006,
          "The generation of the shell script to run the model API has failed.")

MSG_INFO(INFO_LLM_GENERATE_MODEL_API, 9007,
         "Generating REST API for model $0 with class $1.")

}
