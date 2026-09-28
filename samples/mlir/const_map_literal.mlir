module {
  func.func @main() {
    %m = db.constant({age = 32, name = "sam", stats = {height = 10, weight = 10}})

    db.output(%m)

    return
  }
}
