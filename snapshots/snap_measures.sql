{% snapshot snap_measures %}

{{
    config(
        target_schema='main',
        unique_key='notation',
        strategy='check',
        check_cols='all',
    )
}}

SELECT * FROM {{ ref('stg_measures') }}

{% endsnapshot %}
