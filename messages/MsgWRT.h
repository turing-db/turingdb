#pragma once

#include "Message.h"

namespace msg {

MSG_ERROR(ERROR_WRT_BAD_NUMBER_OF_JOBS, 8001,
          "The -j option expects a valid number of jobs, not '$0'")

}
