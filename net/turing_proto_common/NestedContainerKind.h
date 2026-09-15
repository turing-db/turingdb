#pragma once

#include <stdint.h>

namespace net::proto {

//The types of nested containers types we have. As of now nested containers can
//contain each other.
enum class NestedContainerKind : uint8_t {
    List,
    Map,
};

}
