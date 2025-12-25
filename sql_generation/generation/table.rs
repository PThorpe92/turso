use std::sync::atomic::{AtomicU64, Ordering};

use indexmap::IndexSet;
use rand::Rng;
use turso_parser::ast::ColumnConstraint;

use crate::generation::{pick, readable_name_custom, Arbitrary, GenerationContext};
use crate::model::table::{Column, ColumnType, ForeignKeyAction, ForeignKeyConstraint, Name, Table};

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl Arbitrary for Name {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _c: &C) -> Self {
        let base = readable_name_custom("_", rng).replace("-", "_");
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        Name(format!("{base}_{id}"))
    }
}

impl Table {
    /// Generate a table with some predefined columns
    pub fn arbitrary_with_columns<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        name: String,
        predefined_columns: Vec<Column>,
    ) -> Self {
        let opts = context.opts().table.clone();
        let large_table =
            opts.large_table.enable && rng.random_bool(opts.large_table.large_table_prob);
        let target_column_size = if large_table {
            rng.random_range(opts.large_table.column_range)
        } else {
            rng.random_range(opts.column_range)
        } as usize;

        // Start with predefined columns
        let mut column_set = IndexSet::with_capacity(target_column_size);
        for col in predefined_columns {
            column_set.insert(col);
        }

        // Generate additional columns to reach target size
        for col in std::iter::repeat_with(|| Column::arbitrary(rng, context)) {
            column_set.insert(col);
            if column_set.len() >= target_column_size {
                break;
            }
        }

        Table {
            rows: Vec::new(),
            name,
            columns: Vec::from_iter(column_set),
            indexes: vec![],
            foreign_keys: vec![],
        }
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        Table::arbitrary_with_columns(rng, context, name, vec![])
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self {
        let name = Name::arbitrary(rng, context).0;
        let column_type = ColumnType::arbitrary(rng, context);
        Self {
            name,
            column_type,
            constraints: vec![], // TODO: later implement arbitrary here for ColumnConstraint
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}

impl Arbitrary for ForeignKeyAction {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, _context: &C) -> Self {
        pick(
            &[
                Self::NoAction,
                Self::Restrict,
                Self::Cascade,
                Self::SetNull,
                Self::SetDefault,
            ],
            rng,
        )
        .to_owned()
    }
}

/// Result of generating a parent-child table pair with FK relationship
#[derive(Debug, Clone)]
pub struct FkTablePair {
    /// The parent (referenced) table with PRIMARY KEY
    pub parent: Table,
    /// The child table with FOREIGN KEY referencing the parent
    pub child: Table,
    /// The FK constraint on the child table
    pub fk_constraint: ForeignKeyConstraint,
}

impl FkTablePair {
    /// Generate a parent-child table pair with a foreign key relationship.
    ///
    /// The parent table will have an INTEGER PRIMARY KEY column.
    /// The child table will have an INTEGER column that references the parent's PK.
    pub fn generate<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    ) -> Self {
        // Generate parent table with a PRIMARY KEY column
        let parent_name = Name::arbitrary(rng, context).0;
        let parent_pk_col_name = format!("{}_pk", parent_name);

        let parent_pk_column = Column {
            name: parent_pk_col_name.clone(),
            column_type: ColumnType::Integer,
            constraints: vec![ColumnConstraint::PrimaryKey {
                auto_increment: false,
                conflict_clause: None,
                order: None,
            }],
        };

        let parent = Table::arbitrary_with_columns(
            rng,
            context,
            parent_name.clone(),
            vec![parent_pk_column],
        );

        // Generate child table with an FK column referencing parent's PK
        let child_name = Name::arbitrary(rng, context).0;
        let child_fk_col_name = format!("{}_fk", parent_name);

        let child_fk_column = Column {
            name: child_fk_col_name.clone(),
            column_type: ColumnType::Integer,
            constraints: vec![], // FK is defined as table constraint, not column constraint
        };

        let fk_constraint = ForeignKeyConstraint {
            child_columns: vec![child_fk_col_name.clone()],
            parent_table: parent_name.clone(),
            parent_columns: vec![parent_pk_col_name.clone()],
            on_delete,
            on_update,
        };

        let mut child = Table::arbitrary_with_columns(
            rng,
            context,
            child_name,
            vec![child_fk_column],
        );
        child.foreign_keys.push(fk_constraint.clone());

        Self {
            parent,
            child,
            fk_constraint,
        }
    }

    /// Generate a parent-child table pair with random FK actions.
    pub fn generate_random<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        let on_delete = ForeignKeyAction::arbitrary(rng, context);
        let on_update = ForeignKeyAction::arbitrary(rng, context);
        Self::generate(rng, context, on_delete, on_update)
    }

    /// Generate a parent-child pair specifically for CASCADE testing.
    pub fn generate_cascade<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        Self::generate(rng, context, ForeignKeyAction::Cascade, ForeignKeyAction::Cascade)
    }

    /// Generate a parent-child pair specifically for SET NULL testing.
    pub fn generate_set_null<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        Self::generate(rng, context, ForeignKeyAction::SetNull, ForeignKeyAction::SetNull)
    }

    /// Generate a parent-child pair specifically for SET DEFAULT testing.
    pub fn generate_set_default<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        Self::generate(rng, context, ForeignKeyAction::SetDefault, ForeignKeyAction::SetDefault)
    }

    /// Generate a parent-child pair specifically for RESTRICT testing.
    pub fn generate_restrict<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        Self::generate(rng, context, ForeignKeyAction::Restrict, ForeignKeyAction::Restrict)
    }

    /// Generate a parent-child pair specifically for NO ACTION testing.
    pub fn generate_no_action<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
    ) -> Self {
        Self::generate(rng, context, ForeignKeyAction::NoAction, ForeignKeyAction::NoAction)
    }
}
