#pragma once

#ifdef CALLGRIND_PROFILE
#include <valgrind/callgrind.h>
#endif

#ifdef CALLGRIND_PROFILE

#define CALLGRIND_START()   CALLGRIND_START_INSTRUMENTATION
#define CALLGRIND_STOP()    CALLGRIND_STOP_INSTRUMENTATION; CALLGRIND_DUMP_STATS

#else

#define CALLGRIND_START()   {}
#define CALLGRIND_STOP()     {}

#endif