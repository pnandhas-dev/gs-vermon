FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn==21.2.0

COPY agent.py /app/

EXPOSE 8080

# Use gunicorn with 1 worker + threads.
# The WS worker daemon thread starts inside agent.py's __main__ block,
# but gunicorn imports app without calling __main__.
# We use a gunicorn post-fork hook via entrypoint instead — or run directly.
CMD ["python", "agent.py"]
