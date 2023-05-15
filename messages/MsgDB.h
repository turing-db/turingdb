#include "Message.h"

namespace msg {

MSG_INFO(INFO_DB_DUMPING_DATABASE, 5001,
         "Saving Turing DB to $0")

MSG_INFO(INFO_DB_LOADING_DATABASE, 5002,
         "Loading Turing DB from $0")

MSG_INFO(ERROR_DB_DUMPING_DATABASE, 5003,
         "Could not dump Turing DB to $0")

MSG_INFO(ERROR_DB_LOADING_DATABASE, 5004,
         "Could not load Turing DB from $0")

}
