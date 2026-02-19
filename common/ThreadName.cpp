#include "ThreadName.h"

#include <pthread.h>

namespace db {

void ThreadName::set(const char* name) {
#ifdef __APPLE__
    pthread_setname_np(name);
#else
    pthread_setname_np(pthread_self(), name);
#endif
}

}
