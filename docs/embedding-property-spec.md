# Embedding Property Storage Specification

Store fixed-dimension float embeddings as a first-class property type, with
column types for the query pipeline and a specialized property container for
persistent storage.

---

## 1. Type System

### 1.1 ValueType

Add `Embedding` to the existing enum.

```cpp
// storage/metadata/PropertyType.h

enum class ValueType : uint8_t {
    Invalid = 0,
    Int64,
    UInt64,
    Double,
    String,
    Bool,
    Embedding,      // NEW

    _SIZE,
};
```

Update `ValueTypeName` with an `EnumStringPair` for Embedding.
Update `static_assert((size_t)ValueType::_SIZE == 6)` in `EntityPropertyView.h`
to `== 7`.

### 1.2 EmbeddingPrecision

New file: `storage/metadata/EmbeddingPrecision.h`

```cpp
enum class EmbeddingPrecision : uint8_t {
    Float32 = 0,
    // Future: Float16, BFloat16, Int8
};
```

Only `Float32` is implemented. Runtime assert on anything else. The enum exists
so the serialization format and metadata carry precision from day one — no
format migration when other precisions land.

### 1.3 EmbeddingPropertyConfig

New file: `storage/metadata/EmbeddingPropertyConfig.h`

```cpp
struct EmbeddingPropertyConfig {
    uint32_t _dimension;
    EmbeddingPrecision _precision {EmbeddingPrecision::Float32};
};
```

Stored as a side map in `PropertyTypeMap`, keyed by `PropertyTypeID`. Keeps the
lightweight `PropertyType` struct unchanged.

```cpp
// storage/metadata/PropertyTypeMap.h  (additions)

class PropertyTypeMap {
    // ... existing members ...
    std::unordered_map<PropertyTypeID, EmbeddingPropertyConfig> _embeddingConfigs;

public:
    PropertyType getOrCreateEmbedding(std::string_view name,
                                      const EmbeddingPropertyConfig& config);
    const EmbeddingPropertyConfig* getEmbeddingConfig(PropertyTypeID ptID) const;
};
```

### 1.4 Type Tag

```cpp
// storage/metadata/PropertyType.h  (addition inside namespace types)

struct Embedding : public PropertyType {
    using Primitive = std::span<const float>;
    using MandatorySpan = std::span<const Primitive>;
    using OptionalSpan = std::span<const std::optional<Primitive>>;
    static constexpr auto _valueType = ValueType::Embedding;
};
```

Satisfies the `SupportedType` concept (derives from `PropertyType`).

Extend the `TrivialSupportedType` concept to exclude Embedding:

```cpp
template <typename T>
concept TrivialSupportedType = SupportedType<T>
    && !std::same_as<T, types::String>
    && !std::same_as<T, types::Embedding>;
```

### 1.5 PropertyVariant

```cpp
// storage/views/PropertyView.h

using PropertyVariant = std::variant<
    const types::Int64::Primitive*,
    const types::UInt64::Primitive*,
    const types::Double::Primitive*,
    const types::String::Primitive*,
    const types::Bool::Primitive*,
    const types::Embedding::Primitive*>;    // NEW: const std::span<const float>*
```

Follows the uniform `const Primitive*` pattern. The pointer targets a span
stored in `EmbeddingContainer::_views`, which is pointer-stable because
embedding data lives in pre-allocated buckets (see section 2.1).

No specialization of `PropertyView::get<T>()` is needed — the existing
generic template works as-is:

```cpp
template <SupportedType T>
const T::Primitive& get() const {
    return *std::get<const typename T::Primitive*>(_value);
}
```

---

## 2. Storage Layer

### 2.1 EmbeddingBucket

New file: `storage/EmbeddingBucket.h`

Pre-allocates a fixed-size float buffer. Once allocated, the buffer never
reallocates, so spans pointing into it are stable for the lifetime of the
bucket. This is the same stability guarantee that `StringBucket` provides for
`string_view`s.

```cpp
class EmbeddingBucket {
public:
    static constexpr size_t BUCKET_SIZE = 256ul * 1024;     // 256 KB
    static constexpr size_t BUCKET_FLOATS = BUCKET_SIZE / sizeof(float);

    static_assert(BUCKET_SIZE <= std::numeric_limits<uint32_t>::max());

    explicit EmbeddingBucket(uint32_t dimension);
    ~EmbeddingBucket() = default;

    EmbeddingBucket(const EmbeddingBucket&) = delete;
    EmbeddingBucket(EmbeddingBucket&&) noexcept = default;
    EmbeddingBucket& operator=(const EmbeddingBucket&) = delete;
    EmbeddingBucket& operator=(EmbeddingBucket&&) noexcept = default;

    std::span<const float> alloc(std::span<const float> embedding);

    uint32_t embeddingCount() const;
    uint32_t availFloats() const;

    const float* data() const;
    uint32_t floatCount() const;        // used floats
    uint32_t dimension() const;

private:
    std::vector<float> _bucket;         // pre-allocated to BUCKET_FLOATS
    uint32_t _dim;
    uint32_t _floatCount {0};
};
```

#### Why pre-allocation gives pointer stability

The constructor allocates the full buffer up front:

```cpp
EmbeddingBucket::EmbeddingBucket(uint32_t dimension)
    : _bucket(BUCKET_FLOATS),
      _dim(dimension)
{
}
```

`alloc()` copies into the existing buffer — no reallocation ever happens:

```cpp
std::span<const float> alloc(std::span<const float> embedding) {
    bioassert(embedding.size() == _dim, "Dimension mismatch");
    bioassert(availFloats() >= _dim, "Embedding does not fit in bucket");

    float* dst = _bucket.data() + _floatCount;
    std::memcpy(dst, embedding.data(), _dim * sizeof(float));
    _floatCount += _dim;

    return { dst, _dim };
}
```

The returned span points into the pre-allocated heap block. When the parent
`std::vector<EmbeddingBucket>` grows and moves this bucket, `std::vector<float>`
move semantics transfer the heap pointer — the address of the float data does
not change.

#### Capacity

For dim=768 (3072 bytes per embedding): `BUCKET_FLOATS / 768 = 85` embeddings
per bucket. Waste at the bucket tail is at most one embedding.

### 2.2 EmbeddingContainer

New file: `storage/EmbeddingContainer.h`

Owns embedding data via buckets. Analogous to `StringContainer`.

```cpp
class EmbeddingContainer {
public:
    using ViewVector = std::vector<std::span<const float>>;
    using BucketVector = std::vector<EmbeddingBucket>;

    explicit EmbeddingContainer(uint32_t dimension);
    ~EmbeddingContainer() = default;

    EmbeddingContainer(EmbeddingContainer&& other) noexcept = default;
    EmbeddingContainer& operator=(EmbeddingContainer&& other) noexcept = default;
    EmbeddingContainer(const EmbeddingContainer&) = delete;
    EmbeddingContainer& operator=(const EmbeddingContainer&) = delete;

    // --- Insertion ----------------------------------------------------

    void alloc(std::span<const float> embedding);

    // --- Access -------------------------------------------------------

    const std::span<const float>& getView(size_t index) const;

    // --- Metrics ------------------------------------------------------

    size_t size() const;                // number of embeddings
    uint32_t dimension() const;

    // --- Bucket access (for dump/load) --------------------------------

    size_t bucketCount() const;
    const EmbeddingBucket& bucket(size_t i) const;
    const BucketVector& buckets() const;

    void addBucket(EmbeddingBucket&& bucket);

    // --- Iteration ----------------------------------------------------

    ViewVector::const_iterator begin() const;
    ViewVector::const_iterator end() const;
    const ViewVector& get() const;

    void clear();

private:
    BucketVector _buckets;
    ViewVector _views;              // stable: each span points into a bucket
    uint32_t _dim;
};
```

#### alloc()

```cpp
void alloc(std::span<const float> embedding) {
    bioassert(embedding.size() == _dim, "Dimension mismatch");

    EmbeddingBucket* bucket = &_buckets.back();
    if (bucket->availFloats() < _dim) {
        bucket = &_buckets.emplace_back(_dim);
    }

    _views.push_back(bucket->alloc(embedding));
}
```

Same pattern as `StringContainer::alloc()`. The span returned by
`EmbeddingBucket::alloc()` points into stable pre-allocated memory and is stored
directly in `_views`.

#### addBucket() (for loader)

```cpp
void addBucket(EmbeddingBucket&& bucket) {
    _buckets.push_back(std::move(bucket));
    auto& b = _buckets.back();

    const size_t count = b.embeddingCount();
    const size_t prevSize = _views.size();
    _views.resize(prevSize + count);

    const float* base = b.data();
    for (size_t i = 0; i < count; i++) {
        _views[prevSize + i] = { base + i * _dim, _dim };
    }
}
```

#### getView()

```cpp
const std::span<const float>& getView(size_t index) const {
    bioassert(index < _views.size(), "Embedding index invalid");
    return _views[index];
}
```

Returns by reference. The span in `_views[index]` points into bucket memory
that never moves. Safe to take the address of.

### 2.3 TypedPropertyContainer\<types::Embedding\>

Full specialization in `storage/properties/PropertyContainer.h`, following the
`types::String` specialization pattern exactly.

```cpp
template <>
class TypedPropertyContainer<types::Embedding> : public PropertyContainer {
public:
    explicit TypedPropertyContainer(const EmbeddingPropertyConfig& config);

    TypedPropertyContainer(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer(TypedPropertyContainer&&) noexcept = default;
    TypedPropertyContainer& operator=(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer& operator=(TypedPropertyContainer&&) noexcept = default;
    ~TypedPropertyContainer() override = default;

    void add(EntityID entityID, std::span<const float> embedding);

    bool has(EntityID entityID) const override;

    const std::span<const float>& get(EntityID entityID) const;
    const std::span<const float>& get(size_t offset) const;

    const std::span<const float>* tryGet(EntityID entityID) const;

    size_t size() const override;
    void sort() override;

    uint32_t dimension() const;
    const EmbeddingContainer& getRawContainer() const;

private:
    friend EmbeddingPropertyContainerLoader;
    friend DataPartMerger;

    EmbeddingContainer _values;

    // Dense path: arithmetic lookup  (see section 2.4)
    bool _isDense {false};
    EntityID _firstID {0};

    // Sparse fallback: hash map
    std::unordered_map<EntityID, size_t> _entityIndexMap;
};
```

All return types match the String specialization: `const Primitive&` from
`get()`, `const Primitive*` from `tryGet()`. No by-value exceptions.

### 2.4 Dense ID Optimization

Within a DataPart, EntityIDs are typically contiguous (all nodes of a label have
the embedding). When this holds, we skip the hash map entirely and compute
offsets arithmetically, matching the pattern used by `NodeContainer` and
`EdgeContainer` (`(id - _firstID).getValue()`).

#### Detection

After `sort()`, `_ids` is sorted. Contiguity check:

```cpp
bool isDense() const {
    if (_ids.empty()) return true;
    return (_ids.back() - _ids.front()).getValue() + 1 == _ids.size();
}
```

One subtraction, no iteration.

#### sort() implementation

```cpp
void sort() override {
    if (_ids.empty()) return;

    EmbeddingContainer newValues(_values.dimension());

    std::vector<size_t> offsets(_ids.size());
    std::iota(offsets.begin(), offsets.end(), 0);

    ranges::sort(
        ranges::views::zip(_ids, offsets),
        [](const auto& a, const auto& b) {
            return std::get<0>(a) < std::get<0>(b);
        });

    for (size_t i : offsets) {
        newValues.alloc(_values.getView(i));
    }

    _values = std::move(newValues);

    _isDense = isDense();
    if (_isDense) {
        _firstID = _ids.front();
        _entityIndexMap.clear();        // free memory
    } else {
        _entityIndexMap.clear();
        _entityIndexMap.reserve(_ids.size());
        for (size_t i = 0; i < _ids.size(); i++) {
            _entityIndexMap[_ids[i]] = i;
        }
    }
}
```

#### Lookup implementations

```cpp
const std::span<const float>& get(EntityID entityID) const {
    if (_isDense) {
        const size_t offset = (entityID - _firstID).getValue();
        return _values.getView(offset);
    }
    const auto it = _entityIndexMap.find(entityID);
    return _values.getView(it->second);
}

const std::span<const float>* tryGet(EntityID entityID) const {
    if (_isDense) {
        const size_t offset = (entityID - _firstID).getValue();
        if (offset >= _values.size()) {
            return nullptr;
        }
        return &_values.getView(offset);
    }
    const auto it = _entityIndexMap.find(entityID);
    if (it == _entityIndexMap.end()) {
        return nullptr;
    }
    return &_values.getView(it->second);
}
```

All returns are by reference or pointer to stable `_views` entries.
The branch predictor locks onto one path since `_isDense` is constant after
`sort()`.

### 2.5 PropertyManager

```cpp
// storage/properties/PropertyManager.h  (additions)

class PropertyManager {
public:
    void registerEmbeddingPropertyType(PropertyTypeID ptID,
                                       const EmbeddingPropertyConfig& config);
    // ...
private:
    PropertyContainerReferences _embeddings;    // NEW cache
};
```

Update `fillEntityPropertyView()` to iterate `_embeddings` and produce
`PropertyView` entries with `const types::Embedding::Primitive*`.

---

## 3. Query Pipeline Columns

### 3.1 ColumnEmbeddingMany

New file: `storage/columns/ColumnEmbeddingMany.h`

Non-templated container type (like `ColumnMask`). Stores one embedding per row.
Uses a flat `std::vector<float>` — **not** buckets. Columns are transient
pipeline objects; contiguous memory is better for SIMD and cache.

```cpp
class ColumnEmbeddingMany : public Column {
public:
    explicit ColumnEmbeddingMany(uint32_t dimension);
    ~ColumnEmbeddingMany() override = default;

    ColumnEmbeddingMany(const ColumnEmbeddingMany&) = default;
    ColumnEmbeddingMany(ColumnEmbeddingMany&&) noexcept = default;
    ColumnEmbeddingMany& operator=(const ColumnEmbeddingMany&) = default;
    ColumnEmbeddingMany& operator=(ColumnEmbeddingMany&&) noexcept = default;

    // --- Access (returns span by value, 16 bytes = 2 registers) -------

    std::span<const float> operator[](size_t i) const;
    std::span<const float> at(size_t i) const;

    size_t size() const override;           // number of embeddings
    uint32_t dimension() const;

    // --- Mutation ------------------------------------------------------

    void push_back(std::span<const float> embedding);
    void reserve(size_t count);             // reserves count * dim floats

    // --- Raw data (for SIMD bulk ops) ----------------------------------

    float* data();
    const float* data() const;
    size_t dataSize() const;                // total floats

    // --- Column interface ----------------------------------------------

    void assign(const Column* other) override;
    void assignFromLine(const Column* other,
                        size_t startLine, size_t rowCount) override;
    void clear();
    void dump(std::ostream& out) const override;

    std::vector<float>& getRaw();
    const std::vector<float>& getRaw() const;

    static consteval auto staticKind() { return _staticKind; }
    ContainerKind::Code getContainerKind() const override;
    InternalKind::Code getInternalKind() const override { return 0; }
    std::string_view getTypeName() const override;

private:
    std::vector<float> _data;               // flat: N x dim
    uint32_t _dim;

    static constexpr auto _staticKind = ColumnKind::code<ColumnEmbeddingMany>();
};
```

Spans are computed inline: `{ _data.data() + i * _dim, _dim }`. No stored
views — columns are short-lived and don't need pointer stability.

#### assignFromLine

Operates directly on the flat array:

```cpp
void assignFromLine(const Column* other, size_t startLine,
                    size_t rowCount) override {
    const auto* o = dynamic_cast<const ColumnEmbeddingMany*>(other);
    bioassert(o && o->_dim == _dim, "Type/dimension mismatch");

    _data.clear();
    const size_t floatStart = startLine * _dim;
    const size_t floatCount = rowCount * _dim;
    const auto s = o->_data.cbegin() + floatStart;
    _data.assign(s, s + floatCount);
}
```

### 3.2 ColumnEmbeddingConst

New file: `storage/columns/ColumnEmbeddingConst.h`

Single embedding broadcast to all rows, like `ColumnConst<T>`.

```cpp
class ColumnEmbeddingConst : public Column {
public:
    ColumnEmbeddingConst();
    explicit ColumnEmbeddingConst(std::vector<float>&& value);
    ~ColumnEmbeddingConst() override = default;

    ColumnEmbeddingConst(const ColumnEmbeddingConst&) = default;
    ColumnEmbeddingConst(ColumnEmbeddingConst&&) noexcept = default;
    ColumnEmbeddingConst& operator=(const ColumnEmbeddingConst&) = default;
    ColumnEmbeddingConst& operator=(ColumnEmbeddingConst&&) noexcept = default;

    std::span<const float> operator[](size_t /*unused*/) const;
    std::span<const float> at(size_t /*unused*/) const;

    size_t size() const override { return 1; }
    uint32_t dimension() const;

    void set(std::vector<float>&& value);

    void assign(const Column* other) override;
    void assignFromLine(const Column* other,
                        size_t startLine, size_t rowCount) override;
    void dump(std::ostream& out) const override;

    std::vector<float>& getRaw();
    const std::vector<float>& getRaw() const;

    static consteval auto staticKind() { return _staticKind; }
    ContainerKind::Code getContainerKind() const override;
    InternalKind::Code getInternalKind() const override { return 0; }
    std::string_view getTypeName() const override;

private:
    std::vector<float> _data;           // dim floats
    static constexpr auto _staticKind = ColumnKind::code<ColumnEmbeddingConst>();
};
```

### 3.3 ContainerKind Registration

```cpp
// storage/columns/ContainerKind.h

class ContainerKind {
public:
    using Types = KindTypes<
        TemplateKind<ColumnVector>,
        TemplateKind<ColumnConst>,
        TemplateKind<ColumnSet>,
        ColumnMask,
        ColumnStringTable,
        ColumnEmbeddingMany,            // NEW
        ColumnEmbeddingConst>;          // NEW
};
```

Both use `getInternalKind() = 0` (same pattern as `ColumnMask`). Their
`ColumnKind::Code` is `(containerCode << 8) | 0`.

---

## 4. Serialization

### 4.1 Embedding Property Dumper/Loader

New files:
- `storage/dump/EmbeddingPropertyContainerDumper.h`
- `storage/dump/EmbeddingPropertyContainerLoader.h`

#### On-disk metadata page layout

```
Field                   Type        Bytes
----------------------------------------------
File header magic       uint64_t    8
ValueType (Embedding)   uint8_t     1
EmbeddingPrecision      uint8_t     1
Dimension               uint32_t    4
Entity count            uint64_t    8
ID page count           uint64_t    8
Data page count         uint64_t    8
(padding to page boundary)
```

The precision byte is present from day one. The loader asserts `Float32`; when
other precisions land, this is the only gate to open.

#### ID pages

Same format as the trivial property types. EntityID values packed into pages.

#### Data pages

Raw float array, page-aligned. Total bytes = `N * dim * sizeof(float)`.
No encoding or compression. Flat layout enables future `mmap` support.

On load, the flat data is split into `EmbeddingBucket`s of `BUCKET_SIZE` each,
and views are rebuilt via `EmbeddingContainer::addBucket()`.

### 4.2 PropertyTypeMap Serialization

Extend `PropertyTypeMapDumper` and `PropertyTypeMapLoader`.

When `ValueType == Embedding`, write additional fields after the standard
`{name, PropertyTypeID, ValueType}` entry:

```
Field                   Type        Bytes
----------------------------------------------
Dimension               uint32_t    4
Precision               uint8_t     1
```

The loader populates the `_embeddingConfigs` side map from these fields.

---

## 5. Memory Overhead

### Per 1M embeddings, dim=768

| Component                 | Dense IDs | Sparse IDs |
|---------------------------|-----------|------------|
| Float data (buckets)      | 3,072 MB  | 3,072 MB   |
| Views (`_views`)          | 16 MB     | 16 MB      |
| Bucket waste (tail)       | ~0.04 MB  | ~0.04 MB   |
| EntityIDs (`_ids`)        | 8 MB      | 8 MB       |
| EntityIndexMap            | **0 MB**  | ~48 MB     |
| **Total overhead**        | **0.78%** | **2.3%**   |

Bucket waste: at most one embedding (3 KB) per bucket, ~11.8K buckets for 1M
embeddings = ~35 KB total.

`ColumnEmbeddingMany` (pipeline) has zero overhead beyond the flat float array.

---

## 6. Query Layer Integration

This section describes how embedding properties flow through the query pipeline
for **CREATE** (writing) and **MATCH … WHERE** (reading / filtering).

### 6.1 CREATE with Embedding Properties

Syntax:

```cypher
CREATE (n:Document {title: "paper", emb: [0.1, 0.2, 0.3]})
```

The value is a Cypher list literal whose elements are all numeric (integer or
double). The system converts integers to float at write time.

#### 6.1.1 Analyzer Changes

**`ExprAnalyzer::analyzeExpr`** — When visiting `Expr::Kind::LIST`, recurse
into each element. If every element is `Integer` or `Double`, set the
expression type to `EvaluatedType::List`. Otherwise throw an error (heterogeneous
lists are not supported).

```cpp
// query/analyzer/ExprAnalyzer.cpp  (case Expr::Kind::LIST)
case Expr::Kind::LIST: {
    auto* listExpr = static_cast<ListExpr*>(expr);
    for (Expr* elem : *listExpr) {
        analyzeExpr(elem);
        const EvaluatedType et = elem->getType();
        if (et != EvaluatedType::Integer && et != EvaluatedType::Double) {
            throwError("Embedding list elements must be numeric", elem);
        }
    }
    expr->setType(EvaluatedType::List);
} break;
```

**`WriteStmtAnalyzer::evaluatedToValueType`** — Map `EvaluatedType::List` to
`ValueType::Embedding`:

```cpp
case EvaluatedType::List:
    return ValueType::Embedding;
```

**`ExprAnalyzer::propTypeCompatible`** — Allow `List` expression for
`Embedding` value type:

```cpp
case EvaluatedType::List:
    return vt == ValueType::Embedding;
```

#### 6.1.2 Plan / Pipeline Generation

**`ExprProgramGenerator::generateExpr`** — Handle `Expr::Kind::LIST` by
creating a `ColumnEmbeddingConst`:

```cpp
case Expr::Kind::LIST:
    return generateListExpr(static_cast<const ListExpr*>(expr));
```

```cpp
Column* ExprProgramGenerator::generateListExpr(const ListExpr* listExpr) {
    std::vector<float> floats;
    floats.reserve(listExpr->size());

    for (const Expr* elem : *listExpr) {
        if (elem->getKind() != Expr::Kind::LITERAL) {
            throw PlannerException("Embedding list elements must be literals");
        }
        const Literal* lit = static_cast<const LiteralExpr*>(elem)->getLiteral();
        if (lit->getKind() == Literal::Kind::DOUBLE) {
            floats.push_back(
                static_cast<float>(static_cast<const DoubleLiteral*>(lit)->getValue()));
        } else if (lit->getKind() == Literal::Kind::INTEGER) {
            floats.push_back(
                static_cast<float>(static_cast<const IntegerLiteral*>(lit)->getValue()));
        } else {
            throw PlannerException("Embedding list elements must be numeric literals");
        }
    }

    auto* col = _gen->memory().alloc<ColumnEmbeddingConst>();
    col->set(std::move(floats));
    return col;
}
```

#### 6.1.3 WriteProcessor

**`getConstPropertyValue`** — Handle `ValueType::Embedding`:

```cpp
case ValueType::Embedding: {
    const auto* casted = dynamic_cast<ColumnEmbeddingConst*>(valueCol);
    bioassert(casted, "Could not get constant embedding property value.");
    return {propID, casted->getRaw()};
}
```

This requires `std::vector<float>` in `SupportedTypeVariant`.

#### 6.1.4 CommitWriteBuffer

**`SupportedTypeVariant`** — Add `std::vector<float>`:

```cpp
using SupportedTypeVariant =
    std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                 types::Double::Primitive, std::string,
                 types::Bool::Primitive,
                 std::vector<float>>;               // NEW: embedding data
```

**`buildPendingNode` / `buildPendingEdge`** — Add a branch in the `std::visit`:

```cpp
} else if constexpr (std::is_same_v<T, std::vector<float>>) {
    builder.addNodeProperty<types::Embedding>(
        nodeID, id, std::span<const float>(val));
}
```

#### 6.1.5 DataPartBuilder

Add template instantiation for `types::Embedding`:

```cpp
INSTANTIATE(types::Embedding);
```

And the same for `GraphWriter` explicit instantiations.

#### 6.1.6 MetadataBuilder

**`getOrCreatePropertyType`** — When `valueType == Embedding`, call the
embedding-specific path on `PropertyTypeMap`. The dimension is determined from
the first value written. A new overload handles this:

```cpp
PropertyType MetadataBuilder::getOrCreateEmbeddingPropertyType(
    std::string_view name, uint32_t dimension) {
    std::unique_lock lock(_spinLock);
    EmbeddingPropertyConfig config {dimension};
    return _metadata->_propTypeMap.getOrCreateEmbedding(name, config);
}
```

The `WriteProcessor` calls this overload instead of the generic one when
`type == ValueType::Embedding`, passing the dimension from the
`ColumnEmbeddingConst`.

---

### 6.2 WHERE with Embedding Properties

Two operations are supported in WHERE predicates:

1. **Equality** — `WHERE n.emb = [0.1, 0.2, 0.3]`
2. **Cosine similarity** — `WHERE cosineSimilarity(n.emb, [0.1, 0.2, 0.3]) > 0.9`

#### 6.2.1 Analyzer — Property Expression for Embedding

**`ExprAnalyzer::analyzePropertyExpr`** — Map `ValueType::Embedding` to
`EvaluatedType::List`:

```cpp
case ValueType::Embedding: {
    type = EvaluatedType::List;
} break;
```

This allows embedding property expressions to participate in comparisons and
function calls that accept `List` arguments.

#### 6.2.2 Equality of Embeddings

**`ExprAnalyzer::analyzeBinaryExpr`** — Allow `Equal`/`NotEqual` when both
sides are `List`:

```cpp
if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
    break;
}
```

This is added inside the `Equal`/`NotEqual` case, after the existing IS NULL
checks.

**Execution**: Equality of two embeddings is element-wise `memcmp`. Since
embeddings live in `ColumnEmbeddingMany` / `ColumnEmbeddingConst` columns,
a new `ColumnOperator` dispatch is needed. The approach uses a dedicated
evaluation function rather than the generic template dispatch (which does not
know about embedding column types).

**`ColumnOperator`** — Add:

```cpp
OP_EMBEDDING_EQUAL,
OP_EMBEDDING_NOT_EQUAL,
```

**`ExprProgramGenerator::allocBinaryResultCol`** — For `OP_EMBEDDING_EQUAL` and
`OP_EMBEDDING_NOT_EQUAL`, allocate a `ColumnMask` directly:

```cpp
case OP_EMBEDDING_EQUAL:
case OP_EMBEDDING_NOT_EQUAL: {
    result = _gen->memory().alloc<ColumnMask>();
} break;
```

**`ExprProgramGenerator::generateBinaryExpr`** — When both operands have type
`EvaluatedType::List`, override the operator mapping:

```cpp
if (binExpr->getLHS()->getType() == EvaluatedType::List
    && binExpr->getRHS()->getType() == EvaluatedType::List) {
    if (binExpr->getOperator() == BinaryOperator::Equal) {
        op = OP_EMBEDDING_EQUAL;
    } else if (binExpr->getOperator() == BinaryOperator::NotEqual) {
        op = OP_EMBEDDING_NOT_EQUAL;
    }
}
```

**`ExprProgram::evalBinaryInstr`** — Evaluate `OP_EMBEDDING_EQUAL` /
`OP_EMBEDDING_NOT_EQUAL` by calling a dedicated function:

```cpp
case OP_EMBEDDING_EQUAL:
    EvalEmbeddingExpr::evalEqual(res, lhs, rhs);
break;
case OP_EMBEDDING_NOT_EQUAL:
    EvalEmbeddingExpr::evalNotEqual(res, lhs, rhs);
break;
```

**`EvalEmbeddingExpr`** (new, in `query/pipeline/processors/EvalEmbeddingExpr.h`):

```cpp
struct EvalEmbeddingExpr {
    static void evalEqual(Column* res, const Column* lhs, const Column* rhs);
    static void evalNotEqual(Column* res, const Column* lhs, const Column* rhs);
    static void evalCosineSimilarity(Column* res,
                                     const Column* lhs, const Column* rhs);
};
```

#### Dimension validation strategy

Dimensions are **not** checked per-row. They are validated **once** at the start
of each eval function, before entering the row loop. Both sides expose their
dimension through the column interface (`ColumnEmbeddingMany::dimension()`,
`ColumnEmbeddingConst::dimension()`). A `bioassert` at the top of each eval
function is sufficient: every embedding within a single column has the same
dimension (enforced at insertion), so comparing two column dimensions is a
single integer comparison.

```cpp
static uint32_t getEmbeddingDimension(const Column* col) {
    if (const auto* many = dynamic_cast<const ColumnEmbeddingMany*>(col)) {
        return many->dimension();
    }
    if (const auto* cnst = dynamic_cast<const ColumnEmbeddingConst*>(col)) {
        return cnst->dimension();
    }
    throw FatalException("Expected embedding column");
}
```

Helper `getEmbeddingSpan` dispatches on column type:

```cpp
static std::span<const float> getEmbeddingSpan(const Column* col, size_t row) {
    if (const auto* many = dynamic_cast<const ColumnEmbeddingMany*>(col)) {
        return (*many)[row];
    }
    if (const auto* cnst = dynamic_cast<const ColumnEmbeddingConst*>(col)) {
        return (*cnst)[row];
    }
    throw FatalException("Expected embedding column");
}
```

`evalEqual` validates dimensions once, then iterates:

```cpp
void EvalEmbeddingExpr::evalEqual(Column* res,
                                  const Column* lhs, const Column* rhs) {
    auto* mask = static_cast<ColumnMask*>(res);

    const uint32_t dimL = getEmbeddingDimension(lhs);
    const uint32_t dimR = getEmbeddingDimension(rhs);
    bioassert(dimL == dimR, "Embedding dimension mismatch: {} vs {}", dimL, dimR);

    const size_t rows = getEmbeddingRowCount(lhs, rhs);
    mask->resize(rows);

    const size_t bytes = dimL * sizeof(float);
    for (size_t i = 0; i < rows; i++) {
        const auto a = getEmbeddingSpan(lhs, i);
        const auto b = getEmbeddingSpan(rhs, i);
        mask->set(i, std::memcmp(a.data(), b.data(), bytes) == 0);
    }
}
```

#### 6.2.3 Cosine Similarity Function

**Syntax:**

```cypher
MATCH (n:Document)
WHERE cosineSimilarity(n.emb, [0.1, 0.2, 0.3]) > 0.9
RETURN n
```

**`FunctionDecls::initDefault`** — Register:

```cpp
FunctionSignature* cosSim = createFunction("cosineSimilarity");
cosSim->setArguments({EvaluatedType::List, EvaluatedType::List});
cosSim->setReturnTypes({{EvaluatedType::Double}});
```

**`ColumnOperator`** — Add:

```cpp
OP_COSINE_SIMILARITY,
```

with `ColumnOperatorType::OPTYPE_BINARY`.

**`ExprProgramGenerator::generateFuncInvocationExpr`** — Handle:

```cpp
if (funcName == "cosineSimilarity") {
    if (args->size() != 2) {
        throw PlannerException("cosineSimilarity() expects 2 arguments");
    }
    Column* lhsCol = generateExpr(args->at(0));
    Column* rhsCol = generateExpr(args->at(1));

    auto* resCol = _gen->memory().alloc<ColumnVector<types::Double::Primitive>>();
    _exprProg->addInstr(OP_COSINE_SIMILARITY, resCol, lhsCol, rhsCol);
    return resCol;
}
```

**`ExprProgram::evalBinaryInstr`** — Dispatch:

```cpp
case OP_COSINE_SIMILARITY:
    EvalEmbeddingExpr::evalCosineSimilarity(res, lhs, rhs);
break;
```

**Implementation** of `evalCosineSimilarity`. Same strategy: validate dimension
once, then iterate rows with no per-row checks:

```cpp
void EvalEmbeddingExpr::evalCosineSimilarity(Column* res,
                                             const Column* lhs,
                                             const Column* rhs) {
    auto* out = static_cast<ColumnVector<types::Double::Primitive>*>(res);

    const uint32_t dimL = getEmbeddingDimension(lhs);
    const uint32_t dimR = getEmbeddingDimension(rhs);
    bioassert(dimL == dimR, "Embedding dimension mismatch: {} vs {}", dimL, dimR);

    const size_t rows = getEmbeddingRowCount(lhs, rhs);
    out->resize(rows);

    const size_t dim = dimL;
    for (size_t i = 0; i < rows; i++) {
        const auto a = getEmbeddingSpan(lhs, i);
        const auto b = getEmbeddingSpan(rhs, i);

        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (size_t j = 0; j < dim; j++) {
            dot   += (double)a[j] * (double)b[j];
            normA += (double)a[j] * (double)a[j];
            normB += (double)b[j] * (double)b[j];
        }

        const double denom = std::sqrt(normA) * std::sqrt(normB);
        (*out)[i] = (denom > 0.0) ? (dot / denom) : 0.0;
    }
}
```

#### 6.2.4 PropertyTypeDispatcher — Embedding Support

The `PropertyTypeDispatcher` in `PipelineGenerator.cpp` currently throws for
`ValueType::Embedding`. Add a case:

```cpp
case db::ValueType::Embedding:
    executor.template operator()<db::types::Embedding>();
break;
```

This enables `translateGetPropertyNode` and `translateGetPropertyWithNullNode`
to dispatch property fetch processors for embedding columns.

#### 6.2.5 GetPropertiesProcessor — Embedding Instantiation

**`GetPropertiesProcessor.cpp`** — Add template instantiations:

```cpp
template class GetPropertiesProcessor<EntityType::Node, types::Embedding>;
template class GetPropertiesProcessor<EntityType::Edge, types::Embedding>;
```

The `GetPropertiesProcessor<Entity, types::Embedding>` uses
`ColumnEmbeddingMany` as its `ColumnValues` type. This requires that the
`GetPropertiesChunkWriter` specialization for `types::Embedding` maps to
`ColumnEmbeddingMany` (see section 6.2.6).

**`PipelineBuilder.cpp`** — Add template instantiations:

```cpp
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::Embedding>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::Embedding>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::Embedding>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::Embedding>(ColumnTag, PropertyType);
```

#### 6.2.6 GetPropertiesIterator — Embedding Specialization

The `GetPropertiesChunkWriter` template maps `SupportedType` to the output
column type. For trivial types this is `ColumnVector<T::Primitive>`. For
`types::String` there is an existing specialization using `ColumnVector<...>`.
For `types::Embedding`, the output column type is `ColumnEmbeddingMany`:

```cpp
template <>
struct GetPropertiesColumnType<types::Embedding> {
    using ColumnValues = ColumnEmbeddingMany;
};
```

The chunk writer's `fill` method retrieves the embedding span from
`EntityPropertyView` and pushes it into the `ColumnEmbeddingMany`:

```cpp
embeddingCol->push_back(view.get<types::Embedding>());
```

---

## 7. Files to Create

### Storage Layer

| File                                                  | Purpose                                       |
|-------------------------------------------------------|-----------------------------------------------|
| `storage/metadata/EmbeddingPrecision.h`               | Precision enum                                |
| `storage/metadata/EmbeddingPropertyConfig.h`          | Config struct (dimension + precision)         |
| `storage/EmbeddingBucket.h`                           | Fixed-size pre-allocated float bucket         |
| `storage/EmbeddingContainer.h`                        | Data owner: buckets + views                   |
| `storage/columns/ColumnEmbeddingMany.h`               | Pipeline column: N embeddings (flat vector)   |
| `storage/columns/ColumnEmbeddingConst.h`              | Pipeline column: 1 broadcast embedding        |
| `storage/dump/EmbeddingPropertyContainerDumper.h`     | Serialization                                 |
| `storage/dump/EmbeddingPropertyContainerLoader.h`     | Deserialization                               |

### Query Layer

| File                                                        | Purpose                                     |
|-------------------------------------------------------------|---------------------------------------------|
| `query/pipeline/processors/EvalEmbeddingExpr.h`            | Embedding equality and cosine similarity    |

## 8. Files to Modify

### Storage Layer

| File                                          | Change                                                       |
|-----------------------------------------------|--------------------------------------------------------------|
| `storage/metadata/PropertyType.h`             | Add `ValueType::Embedding`, `types::Embedding`, update `TrivialSupportedType` |
| `storage/metadata/PropertyTypeMap.h`          | Add `_embeddingConfigs`, `getOrCreateEmbedding()`            |
| `storage/metadata/PropertyTypeMap.cpp`        | Implement `getOrCreateEmbedding()`, `getEmbeddingConfig()`   |
| `storage/properties/PropertyContainer.h`      | Add `TypedPropertyContainer<types::Embedding>` specialization |
| `storage/properties/PropertyManager.h`        | Add `registerEmbeddingPropertyType()`, `_embeddings` cache   |
| `storage/properties/PropertyManager.cpp`      | Handle Embedding in `fillEntityPropertyView()`               |
| `storage/views/PropertyView.h`                | Extend `PropertyVariant` with `const types::Embedding::Primitive*` |
| `storage/views/EntityPropertyView.h`          | Update `static_assert` to `== 7`                             |
| `storage/columns/ContainerKind.h`             | Register `ColumnEmbeddingMany`, `ColumnEmbeddingConst`       |
| `storage/columns/ColumnOperator.h`            | Add `OP_EMBEDDING_EQUAL`, `OP_EMBEDDING_NOT_EQUAL`, `OP_COSINE_SIMILARITY` |
| `storage/dump/PropertyTypeMapDumper.h`        | Serialize `EmbeddingPropertyConfig` for embedding properties |
| `storage/dump/PropertyTypeMapLoader.h`        | Deserialize `EmbeddingPropertyConfig`                        |
| `storage/dump/DataPartDumper.cpp`             | Dispatch to `EmbeddingPropertyContainerDumper`               |
| `storage/dump/DataPartLoader.cpp`             | Dispatch to `EmbeddingPropertyContainerLoader`               |
| `storage/versioning/CommitWriteBuffer.h`      | Add `std::vector<float>` to `SupportedTypeVariant`           |
| `storage/versioning/CommitWriteBuffer.cpp`    | Dispatch embedding in `buildPendingNode` / `buildPendingEdge` |
| `storage/writers/DataPartBuilder.cpp`         | `INSTANTIATE(types::Embedding)`                              |
| `storage/writers/GraphWriter.cpp`             | Instantiate for `types::Embedding`                           |
| `storage/writers/MetadataBuilder.h`           | Add `getOrCreateEmbeddingPropertyType`                       |
| `storage/writers/MetadataBuilder.cpp`         | Implement `getOrCreateEmbeddingPropertyType`                 |
| `storage/iterators/GetPropertiesIterator.h`   | Add `GetPropertiesColumnType<types::Embedding>` specialization |

### Query Layer

| File                                                    | Change                                                                |
|---------------------------------------------------------|-----------------------------------------------------------------------|
| `query/analyzer/ExprAnalyzer.cpp`                       | Analyze `ListExpr` elements; map `Embedding` → `List`; allow `List` in `propTypeCompatible` |
| `query/analyzer/WriteStmtAnalyzer.cpp`                  | Map `List` → `Embedding` in `evaluatedToValueType()`                 |
| `query/AST/FunctionDecls.cpp`                           | Register `cosineSimilarity(List, List) → Double`                     |
| `query/plan/ExprProgramGenerator.h`                     | Declare `generateListExpr`                                           |
| `query/plan/ExprProgramGenerator.cpp`                   | Implement `generateListExpr`; handle `cosineSimilarity`; handle embedding equality |
| `query/plan/PipelineGenerator.cpp`                      | Add `Embedding` case to `PropertyTypeDispatcher`                     |
| `query/pipeline/processors/ExprProgram.cpp`             | Dispatch `OP_EMBEDDING_EQUAL`, `OP_EMBEDDING_NOT_EQUAL`, `OP_COSINE_SIMILARITY` |
| `query/pipeline/processors/WriteProcessor.cpp`          | Handle `Embedding` in `getConstPropertyValue`                        |
| `query/pipeline/processors/GetPropertiesProcessor.cpp`  | Instantiate for `types::Embedding`                                   |
| `query/pipeline/PipelineBuilder.cpp`                    | Instantiate `addGetProperties` / `addGetPropertiesWithNull` for `types::Embedding` |
