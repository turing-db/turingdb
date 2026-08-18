// CALL db.labels() YIELD id, label RETURN id, label

func.func @main() {
  %0 = nl.procedure("db.labels") yields ["id", "label"]
  %1 = nl.procedure_init(%0, (), {}) : (!nl.procedure_state) -> !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>>
  nl.for %arg0, %arg1 in %1 : !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>> {
    nl.output(%arg0, %arg1) names ["id", "label"] : !nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>
  }
  return
}
