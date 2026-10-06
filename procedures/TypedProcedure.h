#pragma once

#include <stddef.h>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "Procedure.h"
#include "ProcedureData.h"
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

template <typename P, typename Base = ProcedureData>
class TypedProcedureData : public Base {
public:
    using Declaration = P;

    template <size_t I>
    ProcedureReturnColumn<P, I>* getReturnColumn() {
        static_assert(I < P::numReturns, "OOB return column access");
        return static_cast<ProcedureReturnColumn<P, I>*>(Base::getReturnColumn(I));
    }
};

template <ProcedureDataType D>
ProcedureData* allocProcedureData() {
    return new D();
}

template <ProcedureDataType D>
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

template <ProcedureDataType D>
Procedure* createTypedProcedure(std::string_view name) {
    using P = typename D::Declaration;

    Procedure* proc = new Procedure(name);
    proc->setExecuteCallback(&P::execute);
    proc->setAllocCallback(&allocProcedureData<D>);
    proc->setDeallocCallback(&deallocProcedureData<D>);
    proc->setHasIndices(std::is_base_of_v<IndexedProcedureData, D>);

    addProcedureReturnValues<P>(proc, std::make_index_sequence<P::numReturns> {});

    return proc;
}

}
