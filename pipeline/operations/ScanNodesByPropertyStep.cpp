#include "ScanNodesByPropertyStep.h"

#include <sstream>

using namespace db;

template <SupportedType T>
ScanNodesByPropertyStep<T>::ScanNodesByPropertyStep(ColumnIDs* nodes,
                                                    PropertyType propType,
                                                    ColumnVector<typename T::Primitive>* propValues)
    : _nodes(nodes),
    _propValues(propValues),
    _propType(propType)
{
}

template <SupportedType T>
ScanNodesByPropertyStep<T>::~ScanNodesByPropertyStep() {
}

template <SupportedType T>
void ScanNodesByPropertyStep<T>::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByPropertyStep";
    ss << " nodes=" << std::hex << _nodes;
    ss << " props=" << std::hex << _propValues;
    descr.assign(ss.str());
}

namespace db {

template class ScanNodesByPropertyStep<types::Bool>;
template class ScanNodesByPropertyStep<types::Int64>;
template class ScanNodesByPropertyStep<types::UInt64>;
template class ScanNodesByPropertyStep<types::Double>;
template class ScanNodesByPropertyStep<types::String>;

}
