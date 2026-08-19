#include <spdlog/spdlog.h>
#include <type_traits>

#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/ListUtils.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace db;

int main() {
    ListBuffer buf;

    const auto getT = [&]<typename T>(const ListElementView view) {
        const T value = view.getAs<T>();

        if constexpr (std::is_same_v<T, CustomBool>) {
            spdlog::info("element: {}", value._boolean);
        } else if constexpr (std::is_same_v<T, types::Embedding::Primitive>) {
            spdlog::info("element: {}", "embedding");
        } else if constexpr (std::is_same_v<T, ListView>) {
            spdlog::info("element: {}", "list");
        } else if constexpr (std::is_same_v<T, PropertyNull>) {
            spdlog::info("element: {}", "null");
        } else {
            spdlog::info("element: {}", value);
        }
    };

    {
        const std::string name {"Cyrus"};

        const types::Int64::Primitive a = 10;
        const types::Double::Primitive b = 11.1;
        const types::Bool::Primitive c = true;
        const types::String::Primitive d = name;

        std::vector<ListBuffer<>::ListItemVariant> listIn {a, b, c, d};

        ListView list = buf.insert(listIn);

        for (const ListElementView e : list) {
            ListTagDispatcher {._tag = e.getTag()}.execute(getT, e);
        }
    }
}
