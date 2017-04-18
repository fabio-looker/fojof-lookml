#Any connection since the model only uses DT literals
connection: "races"

explore: all_the_things {

  hidden: yes
 join: accounts {
   type: full_outer
   sql_on: FALSE ;;
   relationship: one_to_one
   fields: [count, total_employees]
 }
 join: products {
    type: full_outer
    sql_on: FALSE ;;
    relationship: one_to_one
    fields: [count]
  }
  join: managers {
    type: full_outer
    sql_on: FALSE ;;
    relationship: one_to_one
    fields: [count]
  }
  join: pageviews {
    type: full_outer
    sql_on: FALSE ;;
    relationship: one_to_one
    fields: [count]
  }
  join: orders {
    type: full_outer
    sql_on: FALSE ;;
    relationship: one_to_one
    fields: [count,total_sales,
      shipped_date_date,shipped_date_week,shipped_date_month,shipped_date_year,
        order_date_date,  order_date_week,  order_date_month,  order_date_year,
      combined_date_on]
  }

  #"Dimension table" joins
  join: associated_product {
    view_label: "Products"
    fields: [name]
    type: full_outer
    relationship: many_to_one
    from: products
    # This pattern is required when there are multiple levels of
    # normalization, e.g. orders -> products -> accounts.
    # Why? Looker doesn't automatically join in associated_products
    # if a user selects something from both accounts and orders/pageviews,
    # but not from products.
    # So: I am always requiring associated_products, but from:products_or_empty
    # which internally varies if dependent fields are in the query
    # ...Also, liquid's boolean is basic... https://github.com/Shopify/liquid/issues/138
    sql_table_name:
    {%
      if associated_product.name._in_query contains '1=1'
      or orders.count._in_query contains '1=1'
      or orders.total_sales._in_query contains '1=1'
      or pageviews.count._in_query contains '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}products{%
      else
      %}
    (SELECT null as id, null as account_id FROM (SELECT NULL x) p where x IS NOT NULL)
    {% endif %};;
    sql_on: ${associated_product.id}=COALESCE({%
      if products.count._in_query  == '1=1'
      or products.name._in_query  == '1=1'
      %}
      products.id, {% endif %}{%
      if pageviews.count._in_query  == '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}
      pageviews.product_id, {% endif %}{%
      if orders.count._in_query == '1=1'
      or orders.total_sales._in_query == '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}
      orders.product_id, {% endif %}
      NULL) ;;
  }
  join: associated_account {
    view_label: "Accounts"
    from: accounts
    fields: [employees,name]
    type: full_outer
    relationship: many_to_one
    sql_on: ${associated_account.id}=COALESCE({%
      if accounts.count._in_query  == '1=1'
      or accounts.employees._in_query  == '1=1'
      or accounts.name._in_query  == '1=1'
      or accounts.total_employees._in_query  == '1=1'
      %}
      accounts.id, {% endif %}{%
      if products.count._in_query == '1=1'
      %}
      products.account_id, {% endif %}{%
      if managers.count._in_query == '1=1'
      %}
      managers.account_id, {% endif %}{%
      if pageviews.count._in_query == '1=1'
      or orders.count._in_query == '1=1'
      or orders.total_sales.in_query == '1=1'
      or associated_product.name._in_query == '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}
      ${associated_product.account_id}, {% endif %}
      NULL);;
  }
}


view: all_the_things {
  view_label: "[All the things]"
  sql_table_name: (SELECT base FROM (SELECT null as base) as btable WHERE base IS NOT NULL);;

  dimension: base {
    hidden: yes
    sql: ${TABLE}.base ;;
  }
  dimension_group: combined {
    type: time
    timeframes: [date,week,month]
    sql: ${combined_date_internal} ;;
  }
  dimension: combined_date_internal {
    hidden: yes
    sql: COALESCE({%
      if pageviews.count._in_query contains '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}
      pageviews.pv_date, {% endif %}{%
      if orders.count._in_query contains '1=1'
      or orders.total_sales._in_query contains '1=1'
      or all_the_things.product_conversion_rate._in_query contains '1=1'
      %}
      CASE {% parameter orders.combined_date_on %}
      WHEN 'Order Date' THEN orders.order_date
      WHEN 'Shipped Date' THEN orders.shipped_date
      ELSE orders.order_date
      END, {% endif %}
      CAST(NULL as timestamp)
    );;
  }
  measure: product_conversion_rate {
    type: number
    value_format_name: decimal_3
    sql: CASE WHEN ${pageviews.count}<>0 THEN ${orders.count} / ${pageviews.count} ELSE NULL END;;
  }
}


view: accounts {
  derived_table: {
    sql:
     SELECT 1 as id, 'Acme' as name, 80 as employees
     UNION ALL
     SELECT 2 as id, 'Initech' as name, 120 as employees;;
  }
  dimension: id {
    hidden:  yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
  dimension: employees {
    type: number
    sql: ${TABLE}.employees ;;
  }
  measure: total_employees {
    type: number #Keep nulls as nulls
    sql: SUM(${employees}) ;;
  }
  measure: count {
    type: number
    sql: CASE WHEN MIN(${id}) IS NULL THEN NULL ELSE COUNT(${id}) END ;;
  }
}

view: products {
  derived_table: {
    sql:
     SELECT 1 as id, 'Rockets' as name, 1 as account_id
     UNION ALL
     SELECT 2 as id, 'Portable Holes' as name, 1 as account_id
     UNION ALL
     SELECT 3 as id, 'Synergy' as name, 2 as account_id;;
  }
  dimension: id {
    hidden:  yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: account_id {
    hidden:  yes
    type: number
    sql: ${TABLE}.account_id ;;
  }
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
  measure: count {
    type: number
    sql: CASE WHEN MIN(${id}) IS NULL THEN NULL ELSE COUNT(${id}) END;;
  }
}

view: managers {
  derived_table: {
    sql:
     SELECT 1 as id, 'Wakko' as name, 1 as account_id
     UNION ALL
     SELECT 2 as id, 'Yakko' as name, 1 as account_id
     UNION ALL
     SELECT 3 as id, 'Dot' as name, 1 as account_id
     UNION ALL
     SELECT 4 as id, 'Bill' as name, 2 as account_id;;
  }
  dimension: id {
    hidden:  yes
    type: number
    sql: ${TABLE}.id ;;
  }
  dimension: account_id {
    hidden:  yes
    type: number
    sql: ${TABLE}.account_id ;;
  }
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }
  measure: count {
    type: number
    sql: CASE WHEN MIN(${id}) IS NULL THEN NULL ELSE COUNT(${id}) END;;
  }
}

view: pageviews {
  derived_table: {
    sql:
     SELECT CAST('2017-01-02' as timestamp) as pv_date, 1 as product_id
     UNION ALL
     SELECT CAST('2017-01-15' as timestamp) as pv_date, 2 as product_id
     UNION ALL
     SELECT CAST('2017-01-24' as timestamp) as pv_date, 2 as product_id
     UNION ALL
     SELECT CAST('2017-02-02' as timestamp) as pv_date, 1 as product_id
     UNION ALL
     SELECT CAST('2017-02-02' as timestamp) as pv_date, 3 as product_id
     UNION ALL
     SELECT CAST('2017-02-02' as timestamp) as pv_date, NULL as product_id
     UNION ALL
     SELECT CAST('2017-02-21' as timestamp) as pv_date, 2 as product_id
     UNION ALL
     SELECT CAST('2017-03-02' as timestamp) as pv_date, 3 as product_id
     UNION ALL
     SELECT CAST('2017-03-02' as timestamp) as pv_date, NULL as product_id
     UNION ALL
     SELECT CAST('2017-03-15' as timestamp) as pv_date, 2 as product_id
    ;;
  }
  dimension_group: pv_date {
    hidden:  yes
    timeframes: [raw,date,week,month,year]
    type: time
    sql: ${TABLE}.pv_date ;;
  }
  dimension: product_id {
    hidden:  yes
    type: number
    sql: ${TABLE}.product_id ;;
  }
  measure: count {
    type: number
    sql: CASE WHEN MIN(${TABLE}.pv_date) IS NULL THEN NULL ELSE COUNT(${TABLE}.pv_date) END ;;
  }
}


view: orders {
  derived_table: {
    sql:
     SELECT CAST('2017-01-02' as timestamp) as order_date, CAST('2017-01-06' as timestamp) as shipped_date, 1 as product_id, 200 as sale_price
     UNION ALL
     SELECT CAST('2017-02-02' as timestamp) as order_date, CAST('2017-02-02' as timestamp) as shipped_date,  3 as product_id, 1000 as sale_price
     UNION ALL
     SELECT CAST('2017-02-21' as timestamp) as order_date, CAST('2017-03-08' as timestamp) as shipped_date, 2 as product_id, 25 as sale_price
     UNION ALL
     SELECT CAST('2017-03-15' as timestamp) as order_date, CAST('2017-04-02' as timestamp) as shipped_date, 2 as product_id, 50 as sale_price
    ;;
  }
  filter: combined_date_on {
    suggestions: ["Order Date","Shipped Date"]
  }
  dimension_group: order_date {
    timeframes: [raw,date,week,month,year]
    type: time
    sql: ${TABLE}.order_date ;;
  }
  dimension_group: shipped_date {
    timeframes: [raw,date,week,month,year]
    type: time
    sql: ${TABLE}.order_date ;;
  }
  dimension: product_id {
    hidden:  yes
    type: number
    sql: ${TABLE}.product_id ;;
  }
  dimension: sale_price {
    type: number
    value_format_name: usd
    sql: ${TABLE}.sale_price ;;
  }
  measure: count {
    type: number
    sql: CASE WHEN MIN(${TABLE}.order_date) IS NULL THEN NULL ELSE COUNT(${TABLE}.order_date) END ;;
  }
  measure: total_sales {
    type: number #Not sum because we like nulls as nulls
    sql: SUM(${sale_price}) ;;
  }
}
