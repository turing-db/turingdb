#pragma once

namespace turing::test {

[[noreturn]] void exitWithTestOutcome(void (*scenario)(), unsigned timeoutSeconds);

}
