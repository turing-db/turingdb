---
name: dump-object-bytes-pinned
description: Serializers bulk-write trivially-copyable struct/class object bytes directly, layouts pinned by central static_asserts; no staging buffers in dump paths
metadata:
  type: feedback
---

In dump/serialization hot paths (`storage/dump/`), bulk-write contiguous arrays of trivially-copyable structs/classes (`EntityID`, `StringLimits`) as raw object bytes (`reinterpret_cast` to a byte span), and pin every layout assumption with `static_assert`s placed centrally next to the format constants (`PropertyContainerDumpConstants.h`) — `sizeof`, `offsetof` of each field, trivially copyable. Do NOT introduce temporary staging containers to copy values into primitive buffers before writing.

**Why:** User iterated on this during the bulk-write-dump review: an assert-only fix was first rejected for inner-object staging, then staging was rejected too — "Actually serialize class and struct and put back the static asserts so that we don't have to stage anything and have temporary containers." Final rule: zero-copy object-byte writes win; compile-time asserts carry the layout guarantee.

**How to apply:** keep each bulk write a single-line `writeToCurrentPage(std::span<const uint8_t> {reinterpret_cast<const uint8_t*>(span.data()), span.size_bytes()})`; spans of genuine primitives (float, uint64) go through the typed-span overload with no cast. Put the `static_assert`s in the shared constants header all dumper translation units include, not at each call site.
