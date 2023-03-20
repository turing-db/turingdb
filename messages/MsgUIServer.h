#ifndef _MSG_UI_SERVER_
#define _MSG_UI_SERVER_

#include "Message.h"

namespace msg {

MSG_INFO(INFO_DECOMPRESSING_SITE, 3001,
         "Decompressing site")

MSG_ERROR(ERROR_FAILED_TO_DECOMPRESS_SITE, 3002,
          "Impossible to decompress site '$0'")

MSG_INFO(INFO_BUILDING_SITE, 3003,
         "Building site")

MSG_ERROR(ERROR_FAILED_TO_BUILD_SITE, 3004,
          "Failed to build site '$0'")

MSG_INFO(INFO_RUN_NODE_SERVER, 3005,
         "Run nodejs server on port $0")

MSG_INFO(INFO_STOPPING_NODE_SERVER, 3007,
        "Stopping nodejs server")

MSG_ERROR(ERROR_CLEANING_SITE_FAILED, 3008,
        "Cleaning directory '$0' failed.")

MSG_INFO(INFO_CLEANING_SITE, 3009,
         "Cleaning site.")

}

#endif
