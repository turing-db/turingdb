#pragma once

#include "Message.h"

namespace msg {

MSG_INFO(INFO_RPC_SERVER_STARTED, 6001,
         "Server started on $0") 

MSG_ERROR(ERROR_RPC_SERVER_FAILED_TO_START, 6002,
          "Server failed to start on $0")

}
