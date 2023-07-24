FROM quay.io/astronomer/astro-runtime:8.1.0

ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name":"patner_a","api_secret":"mysecret","path":"/tmp/partner_a"}'
