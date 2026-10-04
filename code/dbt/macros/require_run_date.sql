{% macro require_run_date() %}
    {#- The run's logical date, passed by Airflow as --vars '{run_date: "{{ ds }}"}'.
        Never fall back to current_date: a re-run or backfill must compute the same answer. -#}
    {%- set run_date = var('run_date', none) -%}
    {%- if run_date is none and execute -%}
        {{ exceptions.raise_compiler_error("run_date var is required (Airflow passes the run's logical date)") }}
    {%- endif -%}
    {{ return(run_date) }}
{% endmacro %}
