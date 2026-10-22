# Official Apache image, tag and multi-platform digest verified on 2026-10-21.
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu@sha256:702d82ecea50c6f8344d3df753ba26f05ffd9d1d052e180fed9c3d6c04f77730

USER root
COPY infra/spark/requirements.lock /tmp/spark-requirements.lock
RUN python3 -m pip install --no-cache-dir -r /tmp/spark-requirements.lock
COPY infra/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python3 /tmp/fetch_jars.py --directory /opt/spark/jars
RUN mkdir -p /opt/app /opt/spark/events /opt/spark/work /data \
    && chown -R spark:root /opt/app /opt/spark/events /opt/spark/work /data

COPY --chown=spark:root spark_jobs /opt/app/spark_jobs
COPY --chown=spark:root edai_generator /opt/app/edai_generator
COPY --chown=spark:root pipelines /opt/app/pipelines
COPY --chown=spark:root tests /opt/app/tests
COPY --chown=spark:root tests_spark /opt/app/tests_spark
COPY --chown=spark:root infra/spark/run_job.sh /opt/app/run_job.sh
RUN sed -i 's/\r$//' /opt/app/run_job.sh && chmod 755 /opt/app/run_job.sh

ENV PYTHONPATH=/opt/app PYSPARK_PYTHON=python3 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /opt/app
USER spark
