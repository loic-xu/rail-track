# ==============================================================================
# Base Image: Official Apache Airflow image aligned with Python 3.12 runtime
# ==============================================================================
FROM apache/airflow:2.9.3-python3.12

# ==============================================================================
# Security: Enforce principle of least privilege using standard non-root user
# ==============================================================================
USER airflow

# ==============================================================================
# Analytical Dependencies: Install core processing and orchestration packages
#
# Resilience Note:
# dbt-core-experimental-parser 2.0.11 sdist currently fails on PyPI build due to
# an upstream 504 Gateway Timeout on its legacy github.com/dbt-labs/dbt release URL.
# We fetch the official pre-compiled manylinux binary wheel directly via the GitHub
# Releases API asset endpoint, making the build completely self-contained with no
# reliance on local files or flaky upstream redirects.
# ==============================================================================
RUN curl -sL -H "Accept: application/octet-stream" \
    "https://api.github.com/repositories/53548867/releases/assets/705022413" \
    -o /tmp/dbt_core_experimental_parser-2.0.11-py3-none-manylinux_2_28_x86_64.whl \
    && pip install --no-cache-dir /tmp/dbt_core_experimental_parser-2.0.11-py3-none-manylinux_2_28_x86_64.whl \
    && rm -f /tmp/dbt_core_experimental_parser-2.0.11-py3-none-manylinux_2_28_x86_64.whl \
    && pip install --no-cache-dir \
        "duckdb>=2.0.0" \
        "polars>=2.1.0" \
        "dbt-duckdb>=1.11.1" \
        "httpx>=0.28.1" \
        "pydantic>=2.10.0" \
        "python-dotenv>=1.0.0"

# ==============================================================================
# Python Environment: Register workspace source directory on module search path
# Enables native imports for project modules (rail_track.*) mounted at runtime
# ==============================================================================
ENV PYTHONPATH="/opt/airflow/src"
