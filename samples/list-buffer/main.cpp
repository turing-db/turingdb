#include <variant>

#include <spdlog/spdlog.h>

#include "ListBuffer.h"

#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

int main() {

    const auto printListElement = [](const ListBufferElementView& v) {
        const ListBuffer::ListBufferTag tag = v.getTag();
        switch (tag) {
            case ListBuffer::Int: {
                const auto val = v.getAs<types::Int64::Primitive>();
                spdlog::info("INT : {}", val);
            }
            break;

            case ListBuffer::Double: {
                const auto val = v.getAs<types::Double::Primitive>();
                spdlog::info("DBL : {}", val);
            }
            break;

            case ListBuffer::INVALID:
                throw FatalException("Invalid tag");
            break;
        }
    };


    ListBuffer buf;
    { // example with variant vector
        using FakeList = std::vector<std::variant<types::Int64::Primitive, types::Double::Primitive>>;

        // ListBuffer buf;

        FakeList listIn {10, 1.1, 4.5, 2.0};

        ListView view;

        for (auto&& l : listIn) {
            const auto insert = [&](auto&& typed) -> ListBufferElementView {
                return buf.insert(typed);
            };

            ListBufferElementView v = std::visit(insert, l);
            view.push_back(v);
        }

        for (const ListBufferElementView& v : view) {
            printListElement(v);
        }
    }

    spdlog::info("");

    {
        // ListBuffer buf;

        std::vector<types::Int64::Primitive> homogeneousList {0, 1, 2, 3, 4, 10, 99, 0};

        ListView view = buf.insert(homogeneousList);

        for (const ListBufferElementView& v : view) {
            printListElement(v);
        }
    }
}
