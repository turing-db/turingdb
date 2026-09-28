module {
  func.func @main() {
    %m = db.constant({age = 32 : i64, name = "sam" : !storage.string, stats = {height = 10 : i64, weight = 10 : i64}})

    db.output(%m) : !db.column<!storage.map>

    return
  }
}
