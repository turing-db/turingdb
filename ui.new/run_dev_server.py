#!/usr/bin/env python3
import subprocess
import threading


def run_bioserver():
    return subprocess.run("cd && bioserver", shell=True)


def run_npm_start():
    return subprocess.run("cd frontend && npm start", shell=True)


def run_flask():
    return subprocess.run("cd backend && python3 app.py", shell=True)


def run_tailwind():
    return subprocess.run(
        "cd frontend && npx tailwindcss -i ./src/input.css -o ./src/style.css --watch",
        shell=True,
    )


if __name__ == "__main__":
    bioserver_thread = threading.Thread(target=run_bioserver, daemon=True)
    npm_start_thread = threading.Thread(target=run_npm_start, daemon=True)
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    tailwind_thread = threading.Thread(target=run_tailwind, daemon=True)

    bioserver_thread.start()
    npm_start_thread.start()
    flask_thread.start()
    tailwind_thread.start()
    bioserver_thread.join()
