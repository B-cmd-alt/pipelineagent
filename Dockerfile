# PipelineAgent loop image. Stock python, no build step beyond pip.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY pipelineagent ./pipelineagent
COPY fleet.yaml ./fleet.yaml
RUN pip install --quiet ".[bigquery]"
ENV PIPELINEAGENT_FLEET=/app/fleet.yaml PIPELINEAGENT_SITE_DIR=/app/site
ENTRYPOINT ["python", "-m", "pipelineagent"]
CMD ["run"]
