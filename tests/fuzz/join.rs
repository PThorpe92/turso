#[cfg(test)]
mod join_fuzz_tests {
    use crate::helpers;
    use core_tester::common::TempDatabase;
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    #[derive(Clone, Copy)]
    enum JoinFuzzMode {
        Mixed,
        FullOuterOnly,
        RightOnly,
    }

    impl JoinFuzzMode {
        fn as_str(self) -> &'static str {
            match self {
                Self::Mixed => "mixed",
                Self::FullOuterOnly => "full_outer_only",
                Self::RightOnly => "right_only",
            }
        }
    }

    fn join_fuzz_inner(
        db: TempDatabase,
        add_indexes: bool,
        iterations: usize,
        rows: i64,
        mode: JoinFuzzMode,
    ) {
        let (mut rng, seed) = helpers::init_fuzz_test(&format!(
            "join_fuzz_inner (add_indexes={add_indexes}), mode=({})",
            mode.as_str()
        ));
        let builder = helpers::builder_from_db(&db);
        let limbo_db = builder.clone().build();
        let sqlite_db = builder.clone().build();
        let limbo_conn = limbo_db.connect_limbo();
        let sqlite_conn = rusqlite::Connection::open(sqlite_db.path.clone()).unwrap();

        let schema = r#"
        CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t2(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t3(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);
        CREATE TABLE t4(id INTEGER PRIMARY KEY, a INT, b INT, c INT, d INT);"#;

        sqlite_conn.execute_batch(schema).unwrap();
        limbo_conn.prepare_execute_batch(schema).unwrap();

        if add_indexes {
            let index_ddl = r#"
            CREATE INDEX t1_a_idx ON t1(a);
            CREATE INDEX t1_b_idx ON t1(b);
            CREATE INDEX t1_c_idx ON t1(c);
            CREATE INDEX t1_d_idx ON t1(d);

            CREATE INDEX t2_a_idx ON t2(a);
            CREATE INDEX t2_b_idx ON t2(b);
            CREATE INDEX t2_c_idx ON t2(c);
            CREATE INDEX t2_d_idx ON t2(d);

            CREATE INDEX t3_a_idx ON t3(a);
            CREATE INDEX t3_b_idx ON t3(b);
            CREATE INDEX t3_c_idx ON t3(c);
            CREATE INDEX t3_d_idx ON t3(d);

            CREATE INDEX t4_a_idx ON t4(a);
            CREATE INDEX t4_b_idx ON t4(b);
            CREATE INDEX t4_c_idx ON t4(c);
            CREATE INDEX t4_d_idx ON t4(d);
        "#;
            sqlite_conn.execute_batch(index_ddl).unwrap();
            limbo_conn.prepare_execute_batch(index_ddl).unwrap();
        }

        let tables = ["t1", "t2", "t3", "t4"];
        let mut all_inserts: Vec<String> = Vec::new();
        for (t_idx, tname) in tables.iter().enumerate() {
            for i in 0..rows {
                let id = i + 1 + (t_idx as i64) * 10_000;

                // 25% chance of NULL per column.
                let gen_val = |rng: &mut ChaCha8Rng| {
                    if rng.random_range(0..4) == 0 {
                        None
                    } else {
                        Some(rng.random_range(-10..=20))
                    }
                };
                let a = gen_val(&mut rng);
                let b = gen_val(&mut rng);
                let c = gen_val(&mut rng);
                let d = gen_val(&mut rng);

                let fmt_val = |v: Option<i32>| match v {
                    Some(x) => x.to_string(),
                    None => "NULL".to_string(),
                };

                let stmt = format!(
                    "INSERT INTO {tname}(id,a,b,c,d) VALUES ({id}, {a}, {b}, {c}, {d})",
                    a = fmt_val(a),
                    b = fmt_val(b),
                    c = fmt_val(c),
                    d = fmt_val(d),
                );

                sqlite_conn.execute(&stmt, params![]).unwrap();
                limbo_conn.execute(&stmt).unwrap();
                all_inserts.push(stmt);
            }
        }

        let _non_pk_cols = ["a", "b", "c", "d"];

        // Helper to generate a derived table (FROM clause subquery) for a given table
        let gen_derived_table = |rng: &mut ChaCha8Rng,
                                 table: &str,
                                 alias: &str|
         -> (String, Vec<&str>) {
            let kind = rng.random_range(0..4);
            match kind {
                0 => {
                    // Simple passthrough: (SELECT * FROM t) AS alias
                    (
                        format!("(SELECT * FROM {table}) AS {alias}"),
                        vec!["a", "b", "c", "d"],
                    )
                }
                1 => {
                    // Select specific columns with expression: (SELECT a, b, c + d AS cd FROM t) AS alias
                    (
                        format!("(SELECT a, b, c, d, c + d AS cd FROM {table}) AS {alias}"),
                        vec!["a", "b", "c", "d"],
                    )
                }
                2 => {
                    // With aggregate: (SELECT a, sum(b) AS sum_b, count(*) AS cnt FROM t GROUP BY a) AS alias
                    (
                        format!("(SELECT a, sum(b) AS sum_b, max(c) AS max_c, count(*) AS cnt FROM {table} GROUP BY a) AS {alias}"),
                        vec!["a"], // Only 'a' can be used for joins
                    )
                }
                3 => {
                    // With filter: (SELECT * FROM t WHERE a IS NOT NULL) AS alias
                    (
                        format!("(SELECT * FROM {table} WHERE a IS NOT NULL) AS {alias}"),
                        vec!["a", "b", "c", "d"],
                    )
                }
                _ => unreachable!(),
            }
        };

        for iter in 0..iterations {
            if iter % (iterations / 100).max(1) == 0 {
                println!(
                    "join_fuzz_inner(add_indexes={}) iter {}/{}",
                    add_indexes,
                    iter + 1,
                    iterations
                );
            }

            let query = match mode {
                JoinFuzzMode::Mixed => {
                    let num_tables = rng.random_range(2..=4);
                    let used_tables = &tables[..num_tables];

                    // Decide which tables to wrap in derived tables (30% chance each)
                    let use_derived: Vec<bool> =
                        (0..num_tables).map(|_| rng.random_bool(0.3)).collect();

                    // Generate table references (either direct or derived) and track available join columns
                    let mut table_refs: Vec<(String, String, Vec<&str>)> = Vec::new(); // (from_expr, alias, joinable_cols)
                    for (i, &tname) in used_tables.iter().enumerate() {
                        if use_derived[i] {
                            let alias = format!("sub_{tname}");
                            let (derived, cols) = gen_derived_table(&mut rng, tname, &alias);
                            table_refs.push((derived, alias, cols));
                        } else {
                            table_refs.push((
                                tname.to_string(),
                                tname.to_string(),
                                vec!["a", "b", "c", "d"],
                            ));
                        }
                    }

                    let mut select_cols: Vec<String> = Vec::new();
                    for (_, alias, _) in table_refs.iter() {
                        // For derived tables without id column (like aggregates), we can't select id
                        // So we select the first available column for ordering
                        if alias.starts_with("sub_")
                            && use_derived
                                [table_refs.iter().position(|(_, a, _)| a == alias).unwrap()]
                        {
                            // Check if this is an aggregate derived table (kind==2) by checking if only 'a' is joinable
                            let idx = table_refs.iter().position(|(_, a, _)| a == alias).unwrap();
                            if table_refs[idx].2.len() == 1 {
                                select_cols.push(format!("{alias}.a"));
                            } else {
                                select_cols.push(format!("{alias}.a")); // Use 'a' for consistency
                            }
                        } else {
                            select_cols.push(format!("{alias}.id"));
                        }
                    }
                    let select_clause = select_cols.join(", ");

                    let mut from_clause = format!("FROM {}", table_refs[0].0);
                    let mut had_left_join = false;
                    let mut had_right_join = false;
                    for i in 1..num_tables {
                        let (_, left_alias, left_cols) = &table_refs[i - 1];
                        let (right_expr, right_alias, right_cols) = &table_refs[i];

                        // Constraints on RIGHT JOIN:
                        // - Cannot follow LEFT JOIN (unsupported interaction)
                        // - Cannot have subquery on right side (no rowids for RowSet)
                        // - Only allow one RIGHT JOIN per query (chained RIGHT JOINs
                        //   have subtle interactions with unmatched scans)
                        let right_is_subquery = use_derived[i];
                        let left_is_subquery = use_derived[i - 1];
                        let can_right_join = !had_left_join
                            && !had_right_join
                            && !right_is_subquery
                            && !left_is_subquery;
                        let join_type = match rng.random_range(0..3) {
                            0 => "JOIN",
                            1 => {
                                had_left_join = true;
                                "LEFT JOIN"
                            }
                            2 if can_right_join => {
                                had_right_join = true;
                                "RIGHT JOIN"
                            }
                            _ => {
                                had_left_join = true;
                                "LEFT JOIN"
                            }
                        };

                        // Find common joinable columns between left and right
                        let common_cols: Vec<&str> = left_cols
                            .iter()
                            .filter(|c| right_cols.contains(c))
                            .copied()
                            .collect();

                        // If no common columns (e.g., both are aggregates with only 'a'), use 'a'
                        let join_cols = if common_cols.is_empty() {
                            vec!["a"]
                        } else {
                            common_cols
                        };

                        let num_preds = rng.random_range(1..=join_cols.len().min(3));
                        let mut preds = Vec::new();
                        for _ in 0..num_preds {
                            let col = join_cols[rng.random_range(0..join_cols.len())];
                            preds.push(format!("{left_alias}.{col} = {right_alias}.{col}"));
                        }
                        preds.sort();
                        preds.dedup();

                        let on_clause = preds.join(" AND ");
                        from_clause =
                            format!("{from_clause} {join_type} {right_expr} ON {on_clause}");
                    }

                    // WHERE clause: 0..2 predicates on columns available in each table ref
                    let mut where_parts = Vec::new();
                    let num_where = rng.random_range(0..=2);
                    for _ in 0..num_where {
                        let idx = rng.random_range(0..num_tables);
                        let (_, alias, cols) = &table_refs[idx];
                        if cols.is_empty() {
                            continue;
                        }
                        let col = cols[rng.random_range(0..cols.len())];
                        let kind = rng.random_range(0..4);
                        let cond = match kind {
                            0 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} = {val}")
                            }
                            1 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} <> {val}")
                            }
                            2 => format!("{alias}.{col} IS NULL"),
                            3 => format!("{alias}.{col} IS NOT NULL"),
                            _ => unreachable!(),
                        };
                        where_parts.push(cond);
                    }
                    let where_clause = if where_parts.is_empty() {
                        String::new()
                    } else {
                        format!("WHERE {}", where_parts.join(" AND "))
                    };
                    let order_clause = format!("ORDER BY {}", select_cols.join(", "));
                    let limit = 50;
                    format!(
                        "SELECT {select_clause} {from_clause} {where_clause} {order_clause} LIMIT {limit}",
                    )
                }
                JoinFuzzMode::FullOuterOnly => {
                    // FULL OUTER currently supports two-table shapes with at least one
                    // equi-join key in the ON clause. Keep generation constrained
                    // to that supported set.
                    let left_idx = rng.random_range(0..tables.len());
                    let mut right_idx = rng.random_range(0..tables.len() - 1);
                    if right_idx >= left_idx {
                        right_idx += 1;
                    }
                    let left_table = tables[left_idx];
                    let right_table = tables[right_idx];
                    let left_alias = "l";
                    let right_alias = "r";

                    let join_cols = ["a", "b", "c", "d"];
                    let num_eq_preds = rng.random_range(1..=2);
                    let mut on_parts = Vec::new();
                    for _ in 0..num_eq_preds {
                        let col = join_cols[rng.random_range(0..join_cols.len())];
                        on_parts.push(format!("{left_alias}.{col} = {right_alias}.{col}"));
                    }
                    on_parts.sort();
                    on_parts.dedup();

                    let mut where_parts = Vec::new();
                    let num_where = rng.random_range(0..=3);
                    let where_cols = ["id", "a", "b", "c", "d"];
                    for _ in 0..num_where {
                        let alias = if rng.random_range(0..3) < 2 {
                            left_alias
                        } else {
                            right_alias
                        };
                        let col = where_cols[rng.random_range(0..where_cols.len())];
                        let cond = match rng.random_range(0..6) {
                            0 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} = {val}")
                            }
                            1 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} <> {val}")
                            }
                            2 => format!("{alias}.{col} IS NULL"),
                            3 => format!("{alias}.{col} IS NOT NULL"),
                            4 => {
                                let val = rng.random_range(-10..=20);
                                format!("coalesce({left_alias}.a, {right_alias}.a) >= {val}")
                            }
                            5 => format!("coalesce({left_alias}.a, {right_alias}.a) IS NULL"),
                            _ => unreachable!(),
                        };
                        where_parts.push(cond);
                    }

                    let where_clause = if where_parts.is_empty() {
                        String::new()
                    } else {
                        format!("WHERE {}", where_parts.join(" AND "))
                    };
                    let select_clause = format!(
                        "{left_alias}.id, {right_alias}.id, {left_alias}.a, {right_alias}.a"
                    );
                    let order_clause = format!(
                        "ORDER BY coalesce({left_alias}.id, {right_alias}.id), \
                         {left_alias}.a, {right_alias}.a, {left_alias}.id, {right_alias}.id"
                    );
                    let limit = 50;
                    format!(
                        "SELECT {select_clause} \
                         FROM {left_table} AS {left_alias} FULL OUTER JOIN {right_table} AS {right_alias} \
                         ON {} {where_clause} {order_clause} LIMIT {limit}",
                        on_parts.join(" AND ")
                    )
                }
                JoinFuzzMode::RightOnly => {
                    // RIGHT JOIN supports chained plain-table shapes. Keep this
                    // mode focused on RIGHT semantics without derived-table noise.
                    let num_tables = rng.random_range(2..=3);
                    let used_tables = &tables[..num_tables];
                    let aliases: Vec<String> = (0..num_tables).map(|i| format!("r{i}")).collect();
                    let join_cols = ["a", "b", "c", "d"];

                    let mut from_clause = format!("FROM {} AS {}", used_tables[0], aliases[0]);
                    for i in 1..num_tables {
                        let left_alias = &aliases[i - 1];
                        let right_alias = &aliases[i];
                        let num_eq_preds = rng.random_range(1..=2);
                        let mut on_parts = Vec::new();
                        for _ in 0..num_eq_preds {
                            let col = join_cols[rng.random_range(0..join_cols.len())];
                            on_parts.push(format!("{left_alias}.{col} = {right_alias}.{col}"));
                        }
                        on_parts.sort();
                        on_parts.dedup();
                        from_clause = format!(
                            "{from_clause} RIGHT JOIN {} AS {} ON {}",
                            used_tables[i],
                            right_alias,
                            on_parts.join(" AND ")
                        );
                    }

                    let mut select_cols = Vec::new();
                    for alias in aliases.iter() {
                        select_cols.push(format!("{alias}.id"));
                        select_cols.push(format!("{alias}.a"));
                    }
                    let select_clause = select_cols.join(", ");

                    let mut where_parts = Vec::new();
                    let num_where = rng.random_range(0..=3);
                    let where_cols = ["id", "a", "b", "c", "d"];
                    for _ in 0..num_where {
                        let alias = &aliases[rng.random_range(0..aliases.len())];
                        let col = where_cols[rng.random_range(0..where_cols.len())];
                        let cond = match rng.random_range(0..6) {
                            0 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} = {val}")
                            }
                            1 => {
                                let val = rng.random_range(-10..=20);
                                format!("{alias}.{col} <> {val}")
                            }
                            2 => format!("{alias}.{col} IS NULL"),
                            3 => format!("{alias}.{col} IS NOT NULL"),
                            4 => {
                                let a0 = &aliases[rng.random_range(0..aliases.len())];
                                let a1 = &aliases[rng.random_range(0..aliases.len())];
                                format!("coalesce({a0}.a, {a1}.a) IS NULL")
                            }
                            5 => {
                                let a0 = &aliases[rng.random_range(0..aliases.len())];
                                let a1 = &aliases[rng.random_range(0..aliases.len())];
                                let val = rng.random_range(-10..=20);
                                format!("coalesce({a0}.a, {a1}.a) >= {val}")
                            }
                            _ => unreachable!(),
                        };
                        where_parts.push(cond);
                    }
                    let where_clause = if where_parts.is_empty() {
                        String::new()
                    } else {
                        format!("WHERE {}", where_parts.join(" AND "))
                    };
                    let order_clause = format!("ORDER BY {}", select_cols.join(", "));
                    let limit = 50;
                    format!(
                        "SELECT {select_clause} {from_clause} {where_clause} {order_clause} LIMIT {limit}",
                    )
                }
            };
            helpers::log_progress("join_fuzz_inner", iter, iterations, 10);
            helpers::assert_differential(
                &limbo_conn,
                &sqlite_conn,
                &query,
                &format!("SEED: {seed}"),
            );
        }
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_unindexed_keys_outer(db: TempDatabase) {
        join_fuzz_inner(db, false, 1000, 200, JoinFuzzMode::FullOuterOnly);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_indexed_keys_full_outer(db: TempDatabase) {
        join_fuzz_inner(db, true, 1000, 200, JoinFuzzMode::FullOuterOnly);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_unindexed_keys_right(db: TempDatabase) {
        join_fuzz_inner(db, false, 1000, 200, JoinFuzzMode::RightOnly);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_indexed_keys_right(db: TempDatabase) {
        join_fuzz_inner(db, true, 1000, 200, JoinFuzzMode::RightOnly);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_unindexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, false, 1000, 200, JoinFuzzMode::Mixed);
    }

    #[turso_macros::test(mvcc)]
    pub fn join_fuzz_indexed_keys(db: TempDatabase) {
        join_fuzz_inner(db, true, 1000, 200, JoinFuzzMode::Mixed);
    }
}
