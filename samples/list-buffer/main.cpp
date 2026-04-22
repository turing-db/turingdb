#include <string_view>

#include <spdlog/spdlog.h>
#include <type_traits>

#include "ListBuffer.h"
#include "ListBufferTypeTag.h"
#include "ListElementView.h"

#include "metadata/PropertyType.h"

using namespace db;

struct LBEVDispatcher {
    ListBufferTypeTag _tag {ListBufferTypeTag::INVALID};

    auto execute(const auto& executor, const ListElementView view) const {
        switch (_tag) {
            case ListBufferTypeTag::Int:
                return executor.template operator()<types::Int64::Primitive>(view);
            case ListBufferTypeTag::UInt:
                return executor.template operator()<types::UInt64::Primitive>(view);
            case ListBufferTypeTag::Double:
                return executor.template operator()<types::Double::Primitive>(view);
            case ListBufferTypeTag::Bool:
                return executor.template operator()<types::Bool::Primitive>(view);
            case ListBufferTypeTag::String:
                return executor.template operator()<types::String::Primitive>(view);
            case ListBufferTypeTag::Embedding:
                return executor.template operator()<types::Embedding::Primitive>(view);
            case ListBufferTypeTag::INVALID: {
                std::abort();
            }
        }
    }
};

int main() {
    ListBuffer buf;

    const auto getT = [&]<typename T>(const ListElementView view) {
        const T value = view.getAs<T>();

        if constexpr (std::is_same_v<T, CustomBool>) {
            spdlog::info("element: {}", value._boolean);
        } else if constexpr (std::is_same_v<T, types::Embedding::Primitive>) {
            spdlog::info("element: {}", "embedding");
        } else {
            spdlog::info("element: {}", value);
        }
    };

    {
        const std::string name {"Cyrus"};
        std::string_view view {name};

        ListView list = buf.insert(10UL, 11.1, CustomBool {true}, view);

        for (const ListElementView e : list) {
            LBEVDispatcher {._tag = e.getTag()}.execute(getT, e);
        }
    }
}
