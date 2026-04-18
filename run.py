"""Run entrypoint for Never Stelle."""

from app import create_app, start_workers

app = create_app()


if __name__ == "__main__":
    start_workers()
    app.run(host="0.0.0.0", port=8088, debug=False, threaded=True)
