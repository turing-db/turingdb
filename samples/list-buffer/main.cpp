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
    [[maybe_unused]] ListBuffer buf;

    /*
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

        const types::Int64::Primitive a = 10;
        const types::Double::Primitive b = 11.1;
        const types::Bool::Primitive c = true;
        const types::String::Primitive d = name;

        ListView list = buf.insert(a, b, c ,d);

        for (const ListElementView e : list) {
            LBEVDispatcher {._tag = e.getTag()}.execute(getT, e);
        }
    }
    */
}
