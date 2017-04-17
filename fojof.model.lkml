#Any connection since the model only uses DT literals
connection: "races"

explore: all_the_things {
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
    fields: [count,total_sales]
  }

  #"Dimension table" joins
  join: associated_product {
    view_label: "Products"
    from: products
    fields: [name]
    type: left_outer
    relationship: many_to_one
    sql_on: ${associated_product.id}=COALESCE(
      {% if products.count._in_query
         %},products.id {% endif %}
      {% if pageviews.count._in_query
         %},pageviews.product_id {% endif %}
      {% if orders.count._in_query
         or orders.total_sales._in_query
         %},orders.product_id {% endif %}
    NULL);;
  }
  join: associated_account {
    view_label: "Accounts"
    from: accounts
    fields: [employees,name]
    type: left_outer
    relationship: many_to_one
    sql_on: ${associated_account.id}=COALESCE(
      {% if accounts.count._in_query
         or accounts.total_employees._in_query
         %},accounts.id {% endif %}
      {% if products.count._in_query
         %},products.account_id {% endif %}
      {% if managers.count._in_query
         %},managers.account_id {% endif %}
      {% if pageviews.count._in_query
         or orders.count._in_query
         or orders.total_sales.in_query
         or associated_product.name._in_query
         %},associated_product.account_id {% endif %}
    NULL);;
  }

}


view: all_the_things {
  sql_table_name: (SELECT NULL) ;;
  dimension_group: date {
    type: time
    timeframes: [date,week,month]
    sql: ${combined_date} ;;
  }
  dimension: combined_date {
    hidden: yes
    sql: COALSECE(
      {% if pageviews.count._in_query
         %},pageviews.pageview_date {% endif %}
      {% if orders.count._in_query
         or orders.total_sales._in_query
         %},orders.order_date {% endif %}
      NULL
    );;
  }
  measure: product_conversion_rate {
    type: number
    value_format_name: decimal_3
    sql: ${orders.count}::float / ${pageviews.count}::float ;;
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
    type: sum
    sql: ${employees} ;;
  }
  measure: count {
    type: number
    sql: COUNT(${id}) ;;
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
    sql: COUNT(${id}) ;;
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
    sql: COUNT(${id}) ;;
  }
}

view: pageviews {
  derived_table: {
    sql:
     SELECT '2017-01-01'::date as pv_date, 1 as product_id
     UNION ALL
     SELECT '2017-01-15'::date as pv_date, 2 as product_id
     UNION ALL
     SELECT '2017-01-24'::date as pv_date, 2 as product_id
     UNION ALL
     SELECT '2017-02-01'::date as pv_date, 1 as product_id
     UNION ALL
     SELECT '2017-02-01'::date as pv_date, 3 as product_id
     UNION ALL
     SELECT '2017-02-01'::date as pv_date, NULL as product_id
     UNION ALL
     SELECT '2017-02-21'::date as pv_date, 2 as product_id
     UNION ALL
     SELECT '2017-03-01'::date as pv_date, 3 as product_id
     UNION ALL
     SELECT '2017-03-01'::date as pv_date, NULL as product_id
     UNION ALL
     SELECT '2017-03-15'::date as pv_date, 2 as product_id
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
    sql: COUNT(${TABLE}.*) ;;
  }
}


view: orders {
  derived_table: {
    sql:
     SELECT '2017-01-01'::date as order_date, 1 as product_id, 200 as sale_price
     UNION ALL
     SELECT '2017-02-01'::date as order_date, 3 as product_id, 1000 as sale_price
     UNION ALL
     SELECT '2017-02-21'::date as order_date, 2 as product_id, 25 as sale_price
     UNION ALL
     SELECT '2017-03-15'::date as order_date, 2 as product_id, 50 as sale_price
    ;;
  }
  dimension_group: order_date {
    hidden:  yes
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
    sql: COUNT(${TABLE}.*) ;;
  }
  measure: total_sales {
    type: sum
    sql: ${sale_price} ;;
  }
}
