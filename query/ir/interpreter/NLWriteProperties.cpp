#include "NLWriteProperties.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"

#include "IRException.h"

using namespace db;

namespace {

class ConstPropertyExtractor {
public:
    ConstPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                           PropertyTypeID propID,
                           size_t rowCount)
        : _buf(buf),
        _propID(propID),
        _rowCount(rowCount)
    {
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, typed->getRaw());
        }
    }

    void operator()(const ColumnConst<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, std::string(typed->getRaw()));
        }
    }

    void operator()(const ColumnConst<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(_rowCount);
        const types::Embedding::Primitive span = typed->getRaw();
        for (size_t i = 0; i < _rowCount; i++) {
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(span.begin(), span.end()));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
    size_t _rowCount;
};

class VectorPropertyExtractor {
public:
    VectorPropertyExtractor(CommitWriteBuffer::UntypedProperties& buf,
                            PropertyTypeID propID)
        : _buf(buf),
        _propID(propID)
    {
    }

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const T& val : *typed) {
            _buf.emplace_back(_propID, val);
        }
    }

    void operator()(const ColumnVector<types::String::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::String::Primitive val : *typed) {
            _buf.emplace_back(_propID, std::string(val));
        }
    }

    void operator()(const ColumnVector<types::Embedding::Primitive>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const types::Embedding::Primitive val : *typed) {
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(val.begin(), val.end()));
        }
    }

    template <typename T>
    void operator()(const ColumnVector<std::optional<T>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<T>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, *val);
        }
    }

    void operator()(const ColumnVector<std::optional<types::String::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::String::Primitive>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, std::string(*val));
        }
    }

    void operator()(const ColumnVector<std::optional<types::Embedding::Primitive>>* typed) {
        _buf.clear();
        _buf.reserve(typed->size());
        for (const std::optional<types::Embedding::Primitive>& val : *typed) {
            if (!val) {
                throw IRException("Cannot set a property to NULL in CREATE.");
            }
            _buf.emplace_back(_propID, types::Embedding::OwningPrimitive(val->begin(), val->end()));
        }
    }

private:
    CommitWriteBuffer::UntypedProperties& _buf;
    PropertyTypeID _propID;
};

}

size_t db::committedNodeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalNodesAllocated();
}

size_t db::committedEdgeCount(const GraphView* view) {
    if (!view || !view->isValid()) {
        return 0;
    }

    const GraphReader reader = view->read();
    return reader.getTotalEdgesAllocated();
}

void db::extractColumnProperties(const Column* column,
                                 size_t rowCount,
                                 PropertyTypeID propID,
                                 CommitWriteBuffer::UntypedProperties& buf) {
    using Types = WriteProcessorPropertyTypes;

    const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(column->getKind());

    if (containerKind == ContainerKind::code<ColumnConst>()) {
        ConstPropertyExtractor extractor(buf, propID, rowCount);
        ColumnSingleDispatcher<Types::AllowedConst,
                               ConstPropertyExtractor,
                               Types::ExcludedConst>::dispatch(column, extractor);
    } else {
        VectorPropertyExtractor extractor(buf, propID);
        ColumnSingleDispatcher<Types::AllowedVector,
                               VectorPropertyExtractor,
                               Types::ExcludedVector>::dispatch(column, extractor);
    }
}
