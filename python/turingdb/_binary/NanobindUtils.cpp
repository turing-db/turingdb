#include "NanobindUtils.h"

#include <stdint.h>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "EntityList.h"
#include "EntityType.h"
#include "LocalMemory.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "columns/Column.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnDispatcher.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ContainerKind.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

#include "TuringException.h"

namespace pybindings {

void allocColumns(const db::Dataframe* incomingDf,
                  db::Dataframe* bufferedDf,
                  db::DataframeManager* dfMan,
                  db::LocalMemory* localMem,
                  std::vector<std::string>* nameStorage) {
    const auto& srcCols = incomingDf->cols();
    nameStorage->reserve(srcCols.size());
    for (const db::NamedColumn* namedCol : srcCols) {
        const db::Column* srcCol = namedCol->getColumn();
        db::Column* newCol = localMem->allocSame(srcCol);
        db::NamedColumn* newNamedCol = db::NamedColumn::create(dfMan, newCol, dfMan->allocTag());
        // Copy the name into our own storage; the source view points into the
        // chunk buffer which is reused for subsequent chunks.
        nameStorage->emplace_back(namedCol->getName());
        newNamedCol->rename(nameStorage->back());
        bufferedDf->addColumn(newNamedCol);

        // We just need to copy the value over at alloc time for constants
        if (srcCol->getContainerKind() == db::ContainerKind::code<db::ColumnConst>()) {
            newCol->assign(srcCol);
        }
    }
}

void addToColumn(const db::Column* col, db::Column* newCol) {
    // ColumnConst columns are handled once in allocColumns via assign() and
    // skipped in appendDfs, so only ColumnVector kinds reach here. The
    // dispatcher throws a FatalException for any kind it doesn't enumerate,
    // so an unsupported column type still produces a clear failure.
    db::dispatchColumnVector(col, [newCol](const auto* typedCol) {
        using T = typename std::decay_t<decltype(*typedCol)>::ValueType;
        copyColumnVector<T>(typedCol, newCol);
    });
}

void appendDfs(const db::Dataframe* src, db::Dataframe* dst) {
    const auto& srcCols = src->cols();
    const auto& dstCols = dst->cols();

    for (size_t i = 0; i < srcCols.size(); ++i) {
        db::Column* dstCol = dstCols[i]->getColumn();

        // ColumnConsts are copied once - at column alloc time
        if (dstCol->getContainerKind() == db::ContainerKind::code<db::ColumnConst>()) {
            continue;
        }

        addToColumn(srcCols[i]->getColumn(), dstCol);
    }
}

nb::object embeddingToNdarray(std::span<const float> s) {
    std::vector<float> buf(s.begin(), s.end());
    return wrapVectorAsNdarray(std::move(buf));
}

nb::dict dataframeToNumpy(db::Dataframe* df) {
    nb::dict data;
    nb::dict dtypes;
    const size_t rowCount = df->getLogicalRowCount();

    for (const db::NamedColumn* namedCol : df->cols()) {
        const std::string_view name = namedCol->getName();
        const std::string keyStr =
            name.empty() ? "$" + std::to_string(namedCol->getTag().getValue())
                         : std::string(name);

        db::Column* col = namedCol->getColumn();
        nb::object value;
        const char* dtypeName = "object";

        switch (col->getKind()) {
            case db::ColumnVector<db::types::UInt64::Primitive>::staticKind(): {
                auto& src = static_cast<db::ColumnVector<db::types::UInt64::Primitive>*>(col)->getRaw();
                value = wrapVectorAsNdarray(std::move(src));
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::types::Int64::Primitive>::staticKind(): {
                auto& src = static_cast<db::ColumnVector<db::types::Int64::Primitive>*>(col)->getRaw();
                value = wrapVectorAsNdarray(std::move(src));
                dtypeName = "Int64";
                break;
            }
            case db::ColumnVector<db::types::Double::Primitive>::staticKind(): {
                auto& src = static_cast<db::ColumnVector<db::types::Double::Primitive>*>(col)->getRaw();
                value = wrapVectorAsNdarray(std::move(src));
                dtypeName = "Double";
                break;
            }
            case db::ColumnVector<db::types::Bool::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::types::Bool::Primitive>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint8_t>(src, [](const db::CustomBool& b) -> uint8_t { return b._boolean ? 1 : 0; });
                dtypeName = "Bool";
                break;
            }
            case db::ColumnVector<db::NodeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::NodeID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::NodeID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::EdgeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::EdgeID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::EdgeID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::EdgeTypeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::EdgeTypeID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::EdgeTypeID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::PropertyTypeID>::staticKind(): {
                // Widen to uint64 and report as "UInt64" so the HTTP path's
                // numeric dtype matches; a bespoke "PropertyTypeID" name would
                // fall through DTYPE_MAP to object dtype on the Python side.
                const auto& src = static_cast<const db::ColumnVector<db::PropertyTypeID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::PropertyTypeID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::LabelID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::LabelID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::LabelID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::LabelSetID>::staticKind(): {
                // Widen to uint64 and report as "UInt64" so the HTTP path's
                // numeric dtype matches; a bespoke "LabelSetID" name would
                // fall through DTYPE_MAP to object dtype on the Python side.
                const auto& src = static_cast<const db::ColumnVector<db::LabelSetID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::LabelSetID& v) { return v.getValue(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::ChangeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::ChangeID>*>(col)->getRaw();
                value = transformVectorAsNdarray<uint64_t>(src, [](const db::ChangeID& v) { return v.get(); });
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnVector<db::ValueType>::staticKind(): {
                // Surface ValueType columns as their string names ("Int64", "String", …) so
                // they match the HTTP path, which serializes them as strings server-side.
                const auto& src = static_cast<const db::ColumnVector<db::ValueType>*>(col)->getRaw();
                nb::list lst;
                for (db::ValueType v : src) {
                    lst.append(nb::cast(std::string(db::ValueTypeName::value(v))));
                }
                value = lst;
                dtypeName = "String";
                break;
            }
            case db::ColumnVector<db::types::Embedding::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::types::Embedding::Primitive>*>(col)->getRaw();
                nb::list lst;
                for (const auto& s : src) {
                    lst.append(embeddingToNdarray(s));
                }
                value = lst;
                dtypeName = "Embedding";
                break;
            }
            case db::ColumnVector<db::types::String::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::types::String::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "String";
                break;
            }
            case db::ColumnVector<std::string>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<std::string>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "String";
                break;
            }
            case db::ColumnVector<db::EntityList>::staticKind(): {
                const auto& src = static_cast<const db::ColumnVector<db::EntityList>*>(col)->getRaw();
                nb::list lst;
                for (const db::EntityList& entityList : src) {
                    nb::list rowList;
                    for (const db::EntityList::Entry& entry : entityList.getEntries()) {
                        nb::dict d;
                        d["type"] = nb::cast(entry._type == db::EntityType::Node ? "node" : "edge");
                        d["id"] = nb::cast(entry._id.getValue());
                        rowList.append(d);
                    }
                    lst.append(rowList);
                }
                value = lst;
                dtypeName = "EntityList";
                break;
            }

            case db::ColumnOptVector<db::types::UInt64::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::UInt64::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnOptVector<db::types::Int64::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::Int64::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "Int64";
                break;
            }
            case db::ColumnOptVector<db::types::Double::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::Double::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "Double";
                break;
            }
            case db::ColumnOptVector<db::types::Bool::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::Bool::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "Bool";
                break;
            }
            case db::ColumnOptVector<db::types::String::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::String::Primitive>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "String";
                break;
            }
            case db::ColumnOptVector<std::string>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<std::string>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "String";
                break;
            }
            case db::ColumnOptVector<db::NodeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::NodeID>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnOptVector<db::EdgeID>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::EdgeID>*>(col)->getRaw();
                value = vectorAsList(src);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnOptVector<db::types::Embedding::Primitive>::staticKind(): {
                const auto& src = static_cast<const db::ColumnOptVector<db::types::Embedding::Primitive>*>(col)->getRaw();
                nb::list lst;
                for (const auto& v : src) {
                    if (!v) {
                        lst.append(nb::none());
                    } else {
                        lst.append(embeddingToNdarray(*v));
                    }
                }
                value = lst;
                dtypeName = "Embedding";
                break;
            }

            case db::ColumnConst<db::types::UInt64::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::UInt64::Primitive>*>(col)->getRaw();
                value = repeatValueAsNdarray(v, rowCount);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnConst<db::types::Int64::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::Int64::Primitive>*>(col)->getRaw();
                value = repeatValueAsNdarray(v, rowCount);
                dtypeName = "Int64";
                break;
            }
            case db::ColumnConst<db::types::Double::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::Double::Primitive>*>(col)->getRaw();
                value = repeatValueAsNdarray(v, rowCount);
                dtypeName = "Double";
                break;
            }
            case db::ColumnConst<db::types::Bool::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::Bool::Primitive>*>(col)->getRaw();
                const uint8_t byte = v._boolean ? 1 : 0;
                value = repeatValueAsNdarray(byte, rowCount);
                dtypeName = "Bool";
                break;
            }
            case db::ColumnConst<db::NodeID>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::NodeID>*>(col)->getRaw();
                value = repeatValueAsNdarray(v.getValue(), rowCount);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnConst<db::EdgeID>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::EdgeID>*>(col)->getRaw();
                value = repeatValueAsNdarray(v.getValue(), rowCount);
                dtypeName = "UInt64";
                break;
            }
            case db::ColumnConst<db::types::Embedding::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::Embedding::Primitive>*>(col)->getRaw();
                nb::object py = embeddingToNdarray(v);
                nb::list lst;
                for (size_t i = 0; i < rowCount; ++i) {
                    lst.append(py);
                }
                value = lst;
                dtypeName = "Embedding";
                break;
            }
            case db::ColumnConst<db::types::String::Primitive>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<db::types::String::Primitive>*>(col)->getRaw();
                value = repeatValueAsList(v, rowCount);
                dtypeName = "String";
                break;
            }
            case db::ColumnConst<std::string>::staticKind(): {
                const auto& v = static_cast<const db::ColumnConst<std::string>*>(col)->getRaw();
                value = repeatValueAsList(v, rowCount);
                dtypeName = "String";
                break;
            }
            case db::ColumnConst<db::PropertyNull>::staticKind(): {
                nb::list lst;
                for (size_t i = 0; i < rowCount; ++i) {
                    lst.append(nb::none());
                }
                value = lst;
                dtypeName = "Null";
                break;
            }
            default:
                throw TuringException(std::string("dataframeToNumpy: unhandled column kind: ") + std::string(col->getTypeName()));
        }

        const nb::object key = nb::cast(keyStr);
        data[key] = value;
        dtypes[key] = nb::cast(dtypeName);
    }

    nb::dict out;
    out["data"] = data;
    out["dtypes"] = dtypes;
    return out;
}

}
