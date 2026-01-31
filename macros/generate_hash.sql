{% macro generate_hash(columns) %}
    {#
        Macro to generate a hash value from multiple columns
        Useful for creating surrogate keys or identifying unique records
        
        Usage:
            {{ generate_hash(['column1', 'column2', 'column3']) }}
        
        Returns:
            MD5 hash of concatenated column values as a string
    #}
    
    MD5(
        CONCAT(
            {% for column in columns %}
                COALESCE(CAST({{ column }} AS VARCHAR), '')
                {% if not loop.last %}, '|', {% endif %}
            {% endfor %}
        )
    )
    
{% endmacro %}
