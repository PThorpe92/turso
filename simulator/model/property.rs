use serde::{Deserialize, Serialize};
use sql_generation::model::query::{
    Create, Delete, Insert, Select, predicate::Predicate, update::Update,
};
use sql_generation::model::table::ForeignKeyAction;

use crate::model::Query;

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Debug, Clone, Serialize, Deserialize, strum::EnumDiscriminants, strum::IntoStaticStr)]
#[strum_discriminants(derive(strum::EnumIter, strum::IntoStaticStr))]
#[strum(serialize_all = "Train-Case")]
pub enum Property {
    /// Insert-Select is a property in which the inserted row
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the inserted row.
    /// The execution of the property is as follows
    ///     INSERT INTO <t> VALUES (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The inserted row will not be deleted.
    /// - The inserted row will not be updated.
    /// - The table `t` will not be renamed, dropped, or altered.
    InsertValuesSelect {
        /// The insert query
        insert: Insert,
        /// Selected row index
        row_index: usize,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
        /// The select query
        select: Select,
        /// Interactive query information if any
        interactive: Option<InteractiveQueryInfo>,
    },
    /// ReadYourUpdatesBack is a property in which the updated rows
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the updated row.
    /// The execution of the property is as follows
    ///     UPDATE <t> SET <set_cols=set_vals> WHERE <predicate>
    ///     SELECT <set_cols> FROM <t> WHERE <predicate>
    /// These interactions are executed in immediate succession
    /// just to verify the property that our updates did what they
    /// were supposed to do.
    ReadYourUpdatesBack {
        update: Update,
        select: Select,
    },
    /// TableHasExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    TableHasExpectedContent {
        table: String,
    },
    /// AllTablesHaveExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    /// for each table in the simulator model
    AllTableHaveExpectedContent {
        tables: Vec<String>,
    },
    /// Double Create Failure is a property in which creating
    /// the same table twice leads to an error.
    /// The execution of the property is as follows
    ///     CREATE TABLE <t> (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     CREATE TABLE <t> (...) -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - Table `t` will not be renamed or dropped.
    DoubleCreateFailure {
        /// The create query
        create: Create,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
    },
    /// Select Limit is a property in which the select query
    /// has a limit clause that is respected by the query.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t> WHERE <predicate> LIMIT <n>
    /// This property is a single-interaction property.
    /// The interaction has the following constraints;
    /// - The select query will respect the limit clause.
    SelectLimit {
        /// The select query
        select: Select,
    },
    /// Delete-Select is a property in which the deleted row
    /// must not be in the resulting rows of a select query that has a
    /// where clause that matches the deleted row. In practice, `p1` of
    /// the delete query will be used as the predicate for the select query,
    /// hence the select should return NO ROWS.
    /// The execution of the property is as follows
    ///     DELETE FROM <t> WHERE <predicate>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - A row that holds for the predicate will not be inserted.
    /// - The table `t` will not be renamed, dropped, or altered.
    DeleteSelect {
        table: String,
        predicate: Predicate,
        queries: Vec<Query>,
    },
    /// Drop-Select is a property in which selecting from a dropped table
    /// should result in an error.
    /// The execution of the property is as follows
    ///     DROP TABLE <t>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate> -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The table `t` will not be created, no table will be renamed to `t`.
    DropSelect {
        table: String,
        queries: Vec<Query>,
        select: Select,
    },
    /// Select-Select-Optimizer is a property in which we test the optimizer by
    /// running two equivalent select queries, one with `SELECT <predicate> from <t>`
    /// and the other with `SELECT * from <t> WHERE <predicate>`. As highlighted by
    /// Rigger et al. in Non-Optimizing Reference Engine Construction(NoREC), SQLite
    /// tends to optimize `where` statements while keeping the result column expressions
    /// unoptimized. This property is used to test the optimizer. The property is successful
    /// if the two queries return the same number of rows.
    SelectSelectOptimizer {
        table: String,
        predicate: Predicate,
    },
    /// Where-True-False-Null is a property that tests the boolean logic implementation
    /// in the database. It relies on the fact that `P == true || P == false || P == null` should return true,
    /// as SQLite uses a ternary logic system. This property is invented in "Finding Bugs in Database Systems via Query Partitioning"
    /// by Rigger et al. and it is canonically called Ternary Logic Partitioning (TLP).
    WhereTrueFalseNull {
        select: Select,
        predicate: Predicate,
    },
    /// UNION-ALL-Preserves-Cardinality is a property that tests the UNION ALL operator
    /// implementation in the database. It relies on the fact that `SELECT * FROM <t
    /// > WHERE <predicate> UNION ALL SELECT * FROM <t> WHERE <predicate>`
    /// should return the same number of rows as `SELECT <predicate> FROM <t> WHERE <predicate>`.
    /// > The property is succesfull when the UNION ALL of 2 select queries returns the same number of rows
    /// > as the sum of the two select queries.
    UnionAllPreservesCardinality {
        select: Select,
        where_clause: Predicate,
    },
    /// FsyncNoWait is a property which tests if we do not loose any data after not waiting for fsync.
    ///
    /// # Interactions
    /// - Executes the `query` without waiting for fsync
    /// - Drop all connections and Reopen the database
    /// - Execute the `query` again
    /// - Query tables to assert that the values were inserted
    ///
    FsyncNoWait {
        query: Query,
    },
    FaultyQuery {
        query: Query,
    },

    // Foreign Key Action Properties
    // These properties test the various FK action behaviors (CASCADE, SET NULL,
    // SET DEFAULT, RESTRICT, NO ACTION) for DELETE and UPDATE operations.
    /// Test FK action on DELETE: verifies the correct behavior when a parent row is deleted.
    /// Supports CASCADE, SET NULL, SET DEFAULT, RESTRICT, and NO ACTION.
    ///
    /// Execution flow:
    /// 1. CREATE parent table with PRIMARY KEY
    /// 2. CREATE child table with FOREIGN KEY referencing parent
    /// 3. INSERT row into parent
    /// 4. INSERT row into child referencing parent
    /// 5. DELETE parent row
    /// 6. SELECT from child table
    /// 7. ASSERT based on FK action:
    ///    - CASCADE: child row should be deleted
    ///    - SET NULL: child FK column should be NULL
    ///    - SET DEFAULT: child FK column should be default value
    ///    - RESTRICT/NO ACTION: DELETE should fail (error expected)
    ForeignKeyDeleteAction {
        /// The FK action being tested (on_delete)
        action: ForeignKeyAction,
        /// Parent table name
        parent_table: String,
        /// Child table name
        child_table: String,
        /// Name of the FK column in child table
        fk_column: String,
        /// Name of the PK column in parent table
        pk_column: String,
        /// Create statement for parent table
        create_parent: Create,
        /// Create statement for child table
        create_child: Create,
        /// Insert into parent table
        insert_parent: Insert,
        /// Insert into child table (references parent)
        insert_child: Insert,
        /// Delete from parent table
        delete_parent: Delete,
    },

    /// Test FK action on UPDATE: verifies the correct behavior when a parent PK is updated.
    /// Supports CASCADE, SET NULL, SET DEFAULT, RESTRICT, and NO ACTION.
    ///
    /// Execution flow:
    /// 1. CREATE parent table with PRIMARY KEY
    /// 2. CREATE child table with FOREIGN KEY referencing parent
    /// 3. INSERT row into parent
    /// 4. INSERT row into child referencing parent
    /// 5. UPDATE parent PK to new value
    /// 6. SELECT from child table
    /// 7. ASSERT based on FK action:
    ///    - CASCADE: child FK column should have new value
    ///    - SET NULL: child FK column should be NULL
    ///    - SET DEFAULT: child FK column should be default value
    ///    - RESTRICT/NO ACTION: UPDATE should fail (error expected)
    ForeignKeyUpdateAction {
        /// The FK action being tested (on_update)
        action: ForeignKeyAction,
        /// Parent table name
        parent_table: String,
        /// Child table name
        child_table: String,
        /// Name of the FK column in child table
        fk_column: String,
        /// Name of the PK column in parent table
        pk_column: String,
        /// Create statement for parent table
        create_parent: Create,
        /// Create statement for child table
        create_child: Create,
        /// Insert into parent table
        insert_parent: Insert,
        /// Insert into child table (references parent)
        insert_child: Insert,
        /// Update parent table PK
        update_parent: Update,
        /// The new PK value after update (for CASCADE verification)
        new_pk_value: sql_generation::model::table::SimValue,
    },

    /// Test FK constraint enforcement on INSERT: verifies that inserting a child row
    /// with a non-existent parent FK value fails.
    ///
    /// Execution flow:
    /// 1. CREATE parent table with PRIMARY KEY
    /// 2. CREATE child table with FOREIGN KEY referencing parent
    /// 3. INSERT row into child referencing non-existent parent (should fail)
    ForeignKeyInvalidInsert {
        /// Parent table name
        parent_table: String,
        /// Child table name
        child_table: String,
        /// Create statement for parent table
        create_parent: Create,
        /// Create statement for child table
        create_child: Create,
        /// Insert into child table with invalid FK (should fail)
        insert_child: Insert,
    },

    /// Property used to subsititute a property with its queries only
    Queries {
        queries: Vec<Query>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractiveQueryInfo {
    pub start_with_immediate: bool,
    pub end_with_commit: bool,
}

impl Property {
    /// Property Does some sort of fault injection
    pub fn check_tables(&self) -> bool {
        matches!(
            self,
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. }
        )
    }

    pub fn has_extensional_queries(&self) -> bool {
        matches!(
            self,
            Property::InsertValuesSelect { .. }
                | Property::DoubleCreateFailure { .. }
                | Property::DeleteSelect { .. }
                | Property::DropSelect { .. }
                | Property::Queries { .. }
        )
    }

    pub fn get_extensional_queries(&mut self) -> Option<&mut Vec<Query>> {
        match self {
            Property::InsertValuesSelect { queries, .. }
            | Property::DoubleCreateFailure { queries, .. }
            | Property::DeleteSelect { queries, .. }
            | Property::DropSelect { queries, .. }
            | Property::Queries { queries } => Some(queries),
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. } => None,
            Property::SelectLimit { .. }
            | Property::SelectSelectOptimizer { .. }
            | Property::WhereTrueFalseNull { .. }
            | Property::UnionAllPreservesCardinality { .. }
            | Property::ReadYourUpdatesBack { .. }
            | Property::TableHasExpectedContent { .. }
            | Property::AllTableHaveExpectedContent { .. }
            | Property::ForeignKeyDeleteAction { .. }
            | Property::ForeignKeyUpdateAction { .. }
            | Property::ForeignKeyInvalidInsert { .. } => None,
        }
    }
}

impl PropertyDiscriminants {
    pub fn name(&self) -> &'static str {
        self.into()
    }

    pub fn check_tables(&self) -> bool {
        matches!(
            self,
            Self::AllTableHaveExpectedContent | Self::TableHasExpectedContent
        )
    }
}
