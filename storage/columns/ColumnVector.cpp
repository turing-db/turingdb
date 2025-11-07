#include "ColumnVector.h"

using namespace db;

template <typename T>
template <ColVecInvocable<T> F>
void ColumnVector<T>::apply(F&& fn);

