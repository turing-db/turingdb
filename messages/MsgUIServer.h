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

MSG_INFO(INFO_RUN_FLASK_SERVER, 3008,
         "Run flask server on port $0")

MSG_INFO(INFO_STOPPING_FLASK_SERVER, 3009,
        "Stopping flask server")

MSG_ERROR(ERROR_CLEANING_SITE_FAILED, 3010,
        "Cleaning directory '$0' failed.")

MSG_INFO(INFO_CLEANING_SITE, 3011,
         "Cleaning site.")

MSG_ERROR(ERROR_UI_SERVER_ERROR_OCCURED, 3012,
          "Something went wront during execution of the flask server. Error $0")

MSG_INFO(INFO_UI_SERVER_OUTPUT, 3013,
          "UI Sever logs:\n$0")

}

#endif
