from turingdb import TuringDB


# Helper to create a change and return its ID
def new_change(client: TuringDB) -> int:
    client.checkout()
    change_id = client.new_change()
    client.checkout(change=change_id)
    return change_id


# Helper to submit current change and switch back to main
def submit_current_change(client: TuringDB) -> None:
    client.query("change submit")
    client.checkout()
