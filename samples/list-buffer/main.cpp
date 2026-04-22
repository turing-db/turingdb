#include <string_view>

#include <spdlog/spdlog.h>

#include "ListBuffer.h"
#include "ListElementView.h"

#include "metadata/PropertyType.h"

using namespace db;

int main() {
    ListBuffer buf;

    {
        const std::string name {"Cyrus"};
        std::string_view view {name};

        ListView list = buf.insert(10UL, 11.1, CustomBool {true}, view);

        for (const ListElementView e : list) {
            spdlog::info("element {}", fmt::ptr(&e));
        }
    }
}
