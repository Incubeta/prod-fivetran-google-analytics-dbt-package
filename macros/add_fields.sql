{% macro add_fields(camp) %} 
{% set MAX_CUSTOM_FIELDS = 20%}
{%if camp != "null" %}
{% if  var('campaign_segment_separator') is not none and  var('campaign_segment_separator') != "None" %}

{% set def = var('campaign_segment_separator') %}
{% for i in range(15) %}
{{ split_part(string_text=camp, delimiter_text="'"+def+"'", part_number=i+1) }} as campaign_sub_{{i+1}}
{% if not loop.last %},{% endif %}
{% endfor %}

{% elif var('campaign_segment_separator') is defined and var('campaign_segment_separator') is none %}

{% set def = "-" %}
{% for i in range(15) %}
{{ split_part(string_text=camp, delimiter_text="'"+def+"'", part_number=i+1) }} as campaign_sub_{{i+1}}
{% if not loop.last %},{% endif %}
{% endfor %}

{% elif var('campaign_segment_separator') is not none and var('campaign_segment_separator') == "None" %}

{% set def = "-" %}
{% for i in range(15) %}
{{ split_part(string_text=camp, delimiter_text="'"+def+"'", part_number=i+1) }} as campaign_sub_{{i+1}}
{% if not loop.last %},{% endif %}
{% endfor %}

{% endif %}
,
{% endif %}



{% if var('client_account_structure') is not none and var('client_account_structure') != "None"  %}

{% set client = var('client_account_structure') %}  /* loop possible using number */
{% for i in range(10) %}
{{ split_part(string_text="'"+client+"'", delimiter_text="','", part_number=i+1) }} as account_lvl_{{ i+1 }}
{% if not loop.last %},{% endif %}
{% endfor %}

{% elif var('client_account_structure') is not none and var('client_account_structure') == "None" %}

{% for i in range(10) %}
CAST(null as STRING) account_lvl_{{ i+1 }}
{% if not loop.last %},{% endif %}
{% endfor %}

{% endif %}

,


{% if var('custom_fields') is not none and var('custom_fields') != "None"  %}
{% set custom = var('custom_fields').split(',') %}  

{% for i in range(MAX_CUSTOM_FIELDS )%}
{%if i < custom|length %}
{{custom[i]}} as custom_{{ i+1 }}
{% else %}
CAST(null as STRING) custom_{{ i+1 }}
{%endif%}
{% if not loop.last %},{% endif %}
{% endfor %}

{% elif var('custom_fields') is not none and var('custom_fields') == "None" %}
{% for i in range(MAX_CUSTOM_FIELDS) %}
CAST(null as STRING) custom_{{ i+1 }}
{% if not loop.last %},{% endif %}
{% endfor %}
{% endif %}


{% endmacro %}