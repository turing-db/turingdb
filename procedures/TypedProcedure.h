#pragma once

#include <stddef.h>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "Procedure.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureTypeVector.h"
#include "columns/ColumnVector.h"

namespace db {

template <typename C>
struct ProcedureReturnKind;

template <typename T>
struct ProcedureReturnKind<ColumnVector<T>> {
    static constexpr ProcedureType type = ProcedureTypeOf<T>::value;
    static constexpr bool nullable = false;
};

template <typename T>
struct ProcedureReturnKind<ColumnVector<std::optional<T>>> {
    static constexpr ProcedureType type = ProcedureTypeOf<T>::value;
    static constexpr bool nullable = true;
};

template <typename P, size_t I>
using ProcedureReturnColumn = std::tuple_element_t<I, typename P::Returns>;

template <typename P, size_t I>
ProcedureReturnColumn<P, I>* getReturnColumn(ProcedureData* data) {
    return static_cast<ProcedureReturnColumn<P, I>*>(data->getReturnColumn(I));
}

template <typename P>
ProcedureData* allocProcedureData() {
    return new typename P::Data();
}

template <typename P>
void deallocProcedureData(ProcedureData* data) {
    delete data;
}

template <typename C>
void addProcedureReturnValue(Procedure* proc, std::string_view name) {
    using Kind = ProcedureReturnKind<C>;

    if (Kind::nullable) {
        proc->addNullableReturnValue(name, Kind::type);
    } else {
        proc->addReturnValue(name, Kind::type);
    }
}

template <typename P, size_t... I>
void addProcedureReturnValues(Procedure* proc, std::index_sequence<I...>) {
    (addProcedureReturnValue<ProcedureReturnColumn<P, I>>(proc, P::_returnNames[I]), ...);
}

template <typename P>
void registerTypedProcedure(ProcedureNamespace* ns, std::string_view name) {
    Procedure* proc = new Procedure(name);
    proc->setExecuteCallback(&P::execute);
    proc->setAllocCallback(&allocProcedureData<P>);
    proc->setDeallocCallback(&deallocProcedureData<P>);
    proc->setHasIndices(std::is_base_of_v<IndexedProcedureData, typename P::Data>);

    addProcedureReturnValues<P>(proc, std::make_index_sequence<std::tuple_size_v<typename P::Returns>> {});

    ns->addProcedure(proc);
}

}
