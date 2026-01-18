FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# RUN pip install pandas fastparquet sqlalchemy tqdm click psycopg2-binary pyarrow

WORKDIR /code
# Doing this instead of addin "uv" and "run" to the entrypoint variable ensuring that uv runs with every command
ENV PATH="/code/.venv/bin:$PATH" 

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]