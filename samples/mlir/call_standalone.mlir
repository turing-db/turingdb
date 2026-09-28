// CALL db.labels() YIELD id, label RETURN id, label

func.func @main() {
  %0:2 = db.call_procedure("db.labels", {}, {}) yields ["id", "label"] : () -> (!db.column<none>, !db.column<none>)
  db.output(%0#0, %0#1) names ["id", "label"] : !db.column<none>, !db.column<none>
  return
}
