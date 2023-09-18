#pragma once

#include "Message.h"

namespace msg {

MSG_INFO(INFO_RPC_SERVER_STARTED, 6001,
         "RPC Server started on $0")

MSG_ERROR(ERROR_RPC_SERVER_FAILED_TO_START, 6002,
          "RPC Server failed to start on $0")

}
