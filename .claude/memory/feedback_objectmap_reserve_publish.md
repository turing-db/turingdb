---
name: feedback-objectmap-reserve-publish
description: ObjectMap reserve/publish — reserve the name before the load, do the work between, publish after
metadata:
  type: feedback
---

When inserting into an `ObjectMap` (e.g. `GraphManager::_graphs`), call `reserve(name)` BEFORE building/loading the object and `publish(...)` AFTER, with the load/build work happening BETWEEN the two. Reserving and publishing back-to-back (object already fully built beforehand) is wrong — there is no load between them, which defeats the purpose.

**Why:** Project preference from the user (Remy). The reservation must hold the name for the whole duration of the (potentially slow) load: concurrent loads of the same name then fail fast (`reserve` returns an invalid handle), and a failed load automatically frees the slot when the `SlotReservation` is destroyed without publishing. A `reserve`+`publish` pair with nothing in between provides none of that.

**How to apply:** `auto reservation = _graphs.reserve(name); if (!reservation.isValid()) { ...cleanup...; return; }` then build/load the graph, then `reservation.publish(std::move(graph));`. This is how `GraphManager::createGraph` / `loadGraph` / `loadJsonlDB` / `loadGmlDB` / `loadBinaryDB` are written. Don't reintroduce a separate `addGraph(builtGraph)` helper that reserves+publishes after the work is already done.
