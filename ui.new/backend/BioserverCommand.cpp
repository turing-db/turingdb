#include "BioserverCommand.h"

namespace ui {

BioserverCommand::BioserverCommand()
    : ServerCommand("Bioserver")
{
}

BioserverCommand::~BioserverCommand() = default;

void BioserverCommand::run(ProcessGroup& group) {
    _process = std::make_unique<boost::process::child>(
        "bioserver",
        boost::process::std_in.close(),
        boost::process::std_out > boost::process::null,
        boost::process::std_err > boost::process::null,
        *group);
}

}
