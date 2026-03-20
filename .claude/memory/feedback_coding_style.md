---
name: coding_style_preferences
description: User's C++ coding style preferences beyond CODING_STYLE.md — POD defaults, getter naming, no return-by-value for large objects, impl in cpp files
type: feedback
---

- POD member variables must always have a default value (e.g. `size_t _dimension {0};`).
  **Why:** Avoids uninitialized memory bugs.
  **How to apply:** Every scalar/POD member in a class or struct gets a brace-or-equal initializer.

- Use `getX()` style for getter methods (e.g. `getDimension()`, `getCapacity()`).
  **Why:** Consistency with the user's preferred API style.
  **How to apply:** Name all const accessor methods as `getPropertyName()`.

- Don't return large/non-trivial objects by value from factory methods. Use fill/output reference patterns instead.
  **Why:** Avoids unnecessary copies and the user considers return-by-value for large objects distasteful.
  **How to apply:** For construction/loading from disk, take a reference and fill it (or throw on failure) rather than returning `std::optional<T>`.

- Put non-trivial function implementations in .cpp files, not in headers. Keep headers declaration-only.
  **Why:** Reduce header bloat and compilation dependencies.
  **How to apply:** Only trivial one-liner getters should be inline in the header. Anything with logic (alloc, fill, computeCapacity) goes in the .cpp.

- Helper functions that don't need to be in the class interface should be anonymous (static/namespace{}) functions in the .cpp file.
  **Why:** Minimal public API surface.
  **How to apply:** If a function like `computeCapacity` is only used internally, make it a file-local function in the .cpp rather than a public static method.

- Destructors should be declared in the header and defined in the .cpp (not `= default` in the header).
  **Why:** Consistent with project patterns and allows implementation changes without header modification.
  **How to apply:** Declare `~ClassName();` in header, define `ClassName::~ClassName() {}` or equivalent in .cpp.

- Throw `TuringException` on failure rather than returning error codes or optionals.
  **Why:** Consistent with project error handling patterns. Exceptions must derive from TuringException.
  **How to apply:** Use `throw TuringException(...)` for validation failures in methods like `fill()`.
