#include "ScanNodesByPropertyAndLabelStep.h"

#include <sstream>
#include <spdlog/fmt/ranges.h>

using namespace db;

template <SupportedType T>
ScanNodesByPropertyAndLabel<T>::ScanNodesByPropertyAndLabel(ColumnNodeIDs* nodes,
                                                            PropertyType propType,
                                                            const LabelSetHandle& labelSet,
                                                            ColumnVector<typename T::Primitive>* propValues)
    : _nodes(nodes),
    _propValues(propValues),
    _propType(propType),
    _labelSet(labelSet)
{
}

template <SupportedType T>
ScanNodesByPropertyAndLabel<T>::~ScanNodesByPropertyAndLabel() {
}

template <SupportedType T>
void ScanNodesByPropertyAndLabel<T>::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByPropertyAndLabel";
    ss << " nodes=" << std::hex << _nodes;
    ss << " props=" << std::hex << _propValues;
    ss << " propType Id=" << _propType._id;

    std::vector<LabelID> labels;
    _labelSet.decompose(labels);
    ss << fmt::format(" labelSet={}", fmt::join(labels, ", "));

    descr.assign(ss.str());
}

namespace db {

template class ScanNodesByPropertyAndLabel<types::Bool>;
template class ScanNodesByPropertyAndLabel<types::Int64>;
template class ScanNodesByPropertyAndLabel<types::UInt64>;
template class ScanNodesByPropertyAndLabel<types::Double>;
template class ScanNodesByPropertyAndLabel<types::String>;

}
