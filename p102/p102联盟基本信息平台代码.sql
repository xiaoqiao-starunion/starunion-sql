SELECT *
FROM
  (SELECT *
   FROM
     (SELECT tab_ali.*,
             ali_rank.power_order
      FROM
        (SELECT *
         FROM
           (SELECT *
            FROM
              (SELECT row_number() over(
                                        ORDER BY alliance_power DESC) AS rn,
                                   *
               FROM
                 (SELECT *
                  FROM
                    (SELECT *,
                            row_number() over(partition BY alliance_id) rk
                     FROM
                       (SELECT *,
                               COUNT(1) over(partition BY alliance_id)AS member,
                                        SUM(paid) over(partition BY alliance_id)AS alliance_total_paid
                        FROM alliances
                        LEFT JOIN
                          (SELECT roles.alliance_id AS roles_alli_id,
                                  roles.paid
                           FROM roles) AS TEMP ON TEMP.roles_alli_id = alliances.alliance_id) 
                       tab_rank)tab_rank_limit
                  WHERE rk <= 1
                  )AS alliances) AS TEMP
            WHERE rn BETWEEN 1 AND 20)tab,
           (SELECT count(1) AS total_size
            FROM
              (SELECT *
               FROM
                 (SELECT *,
                         row_number() over(partition BY alliance_id) rk
                  FROM
                    (SELECT *,
                            COUNT(1) over(partition BY alliance_id)AS member,
                                     SUM(paid) over(partition BY alliance_id)AS alliance_total_paid
                     FROM alliances
                     LEFT JOIN
                       (SELECT roles.alliance_id AS roles_alli_id,
                               roles.paid
                        FROM roles
                        ) AS TEMP ON TEMP.roles_alli_id = alliances.alliance_id) tab_rank
                    )tab_rank_limit
               WHERE rk <= 1)AS alliances)) tab_ali
      INNER JOIN
        (SELECT alliance_id,
                row_number() over(
                                  ORDER BY alliance_power DESC) AS power_order
         FROM alliances)ali_rank ON ali_rank.alliance_id=tab_ali.alliance_id) tab_ali_info
   LEFT JOIN
     (SELECT alliance_id,
             array_join(array_agg(alliance_orders),'') alliance_orders
      FROM
        (SELECT *
         FROM
           (SELECT *,
                   row_number() over(partition BY alliance_id
                                     ORDER BY num DESC) rank
            FROM
              (SELECT concat(roles.country,'-',cast(count(1) AS varchar),';') AS alliance_orders,
                      count(1) num,
                      alliance_id,
                      country
               FROM roles
               GROUP BY alliance_id,
                        country) tab_rank)tab_rank_limit
         WHERE rank <= 5)tab_ali_agg
      GROUP BY alliance_id)tab_member_regions ON tab_member_regions.alliance_id=tab_ali_info.alliance_id
   ORDER BY alliance_power DESC)tab
LEFT JOIN
  (SELECT alliances.alliance_id AS alli_id,
          count(DISTINCT if(DATE_DIFF('day',
            DATE_PARSE(DATE_FORMAT(FROM_UNIXTIME(log_login_role.created_at) AT TIME ZONE '+00:00' ,'%Y-%m-%d'),'%Y-%m-%d'),
            DATE_PARSE('2021-10-08','%Y-%m-%d'))<=3,role_id,NULL)) active_3
   FROM
     (SELECT *
      FROM alliances)alliances
   INNER JOIN roles ON roles.alliance_id=alliances.alliance_id
   INNER JOIN log_login_role ON log_login_role.role_id=roles.uuid
   GROUP BY alliances.alliance_id) active_tab ON tab.roles_alli_id=active_tab.alli_id
ORDER BY alliance_power DESC