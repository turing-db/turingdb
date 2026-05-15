---
name: feedback-no-redundant-cmake
description: "Don't run `cmake ..` before every `make` — make triggers a reconfigure itself when CMakeLists.txt changes"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 092696f3-f6ed-4852-8093-a14ca7d5b3c4
---

Don't preface `make` with a `cmake ..` invocation by reflex. `make` already re-runs cmake when any tracked CMakeLists.txt has changed since the last configure. Manual `cmake ..` is only useful for the very first configure or for forcing a refresh after editing the cmake cache.

**Why:** User explicit annoyance: "Stop your stupid cmake command everytime." Slowing down the loop with a redundant ~5–15s reconfigure on every iteration.

**How to apply:** When iterating on `.cpp` / `.h` files, just run `make -j8 <target>`. Only run `cmake ..` after editing a CMakeLists.txt or `dependencies.sh`, or when explicitly resetting the build dir.
