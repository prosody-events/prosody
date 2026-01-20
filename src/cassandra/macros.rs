//! Macros for reducing Cassandra boilerplate.
//!
//! This module provides declarative macros that eliminate repetitive code
//! patterns in Cassandra prepared statement management.

use crate::cassandra::errors::CassandraStoreError;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

/// Prepares a CQL statement with optimized settings.
///
/// This is a helper function used by the `cassandra_queries!` macro.
/// It prepares the given CQL statement and applies performance optimizations:
/// - Enables cached result metadata for better performance
/// - Marks the statement as idempotent for safer retries
///
/// # Arguments
///
/// * `session` - The Cassandra session to use for preparation
/// * `statement` - The CQL statement string to prepare
///
/// # Returns
///
/// A [`PreparedStatement`] ready for execution with optimizations applied.
///
/// # Errors
///
/// Returns an error if statement preparation fails.
#[doc(hidden)]
pub(crate) async fn prepare_statement(
    session: &Session,
    statement: &str,
) -> Result<PreparedStatement, CassandraStoreError> {
    let mut statement = session.prepare(statement).await?;
    statement.set_use_cached_result_metadata(true);
    statement.set_is_idempotent(true);

    Ok(statement)
}

/// Helper function to format SQL with keyspace and additional arguments.
///
/// Replaces `$keyspace` with the keyspace value, then formats remaining `{}`
/// placeholders with the provided string arguments.
#[doc(hidden)]
#[must_use]
pub(crate) fn format_sql(template: &str, keyspace: &str, args: &[&str]) -> String {
    let with_keyspace = template.replace("$keyspace", keyspace);

    if args.is_empty() {
        return with_keyspace;
    }

    let mut result = String::new();
    let mut remaining = with_keyspace.as_str();

    for arg in args {
        if let Some(pos) = remaining.find("{}") {
            result.push_str(&remaining[..pos]);
            result.push_str(arg);
            remaining = &remaining[pos + 2..];
        } else {
            break;
        }
    }
    result.push_str(remaining);
    result
}

/// Generates a struct containing prepared Cassandra statements.
///
/// This macro eliminates boilerplate for managing prepared CQL statements by
/// generating:
/// - The struct definition with `PreparedStatement` fields
/// - An async `new()` constructor that prepares all statements
/// - Individual prepare functions for each query
///
/// All generated fields are public and marked with `#[educe(Debug(ignore))]`
/// to prevent prepared statement details from appearing in debug output.
///
/// # Keyspace Interpolation
///
/// Use `$keyspace` in your SQL strings where you want the keyspace name to
/// appear. This placeholder will be replaced with the keyspace parameter from
/// the `new()` function. Use `{}` placeholders for all other runtime
/// parameters.
///
/// # Syntax
///
/// ```rust,ignore
/// cassandra_queries! {
///     /// Documentation for the struct
///     pub struct Queries {
///         /// Documentation for this query
///         field_name: (
///             "SQL with $keyspace and {} placeholders",
///             arg1, arg2, ...
///         ),
///         // ... more queries
///     }
/// }
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use crate::cassandra::TABLE_SEGMENTS;
///
/// cassandra_queries! {
///     /// Container for timer queries
///     pub struct TimerQueries {
///         /// Insert a new segment
///         insert_segment: (
///             "INSERT INTO $keyspace.{} (id, name) VALUES (?, ?)",
///             TABLE_SEGMENTS
///         ),
///
///         /// Get segment by ID
///         get_segment: (
///             "SELECT name FROM $keyspace.{} WHERE id = ?",
///             TABLE_SEGMENTS
///         ),
///     }
/// }
/// ```
///
/// # Requirements
///
/// None - the macro is fully self-contained and handles all necessary imports.
#[macro_export]
macro_rules! cassandra_queries {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident: ($sql:literal $(, $arg:expr)* $(,)?)
            ),* $(,)?
        }
    ) => {
        // Generate the struct definition
        $(#[$struct_meta])*
        #[derive(::educe::Educe)]
        #[educe(Debug)]
        $vis struct $name {
            $(
                $(#[$field_meta])*
                #[educe(Debug(ignore))]
                pub $field: ::scylla::statement::prepared::PreparedStatement,
            )*
        }

        // Generate the impl block with new() function
        impl $name {
            /// Creates a new instance with all prepared statements.
            ///
            /// # Arguments
            ///
            /// * `session` - Cassandra session to use for statement preparation
            /// * `keyspace` - Keyspace name for statement preparation
            ///
            /// # Errors
            ///
            /// Returns an error if any statement preparation fails.
            pub async fn new(
                session: &::scylla::client::session::Session,
                keyspace: &str,
            ) -> ::std::result::Result<Self, $crate::cassandra::errors::CassandraStoreError> {
                $(
                    let $field = ::paste::paste! {
                        [<prepare_ $field>](session, keyspace).await?
                    };
                )*

                ::std::result::Result::Ok(Self {
                    $(
                        $field,
                    )*
                })
            }
        }

        // Generate prepare functions for each query
        $(
            ::paste::paste! {
                /// Prepares the
                #[doc = concat!("`", stringify!($field), "`")]
                /// query.
                async fn [<prepare_ $field>](
                    session: &::scylla::client::session::Session,
                    keyspace: &str,
                ) -> ::std::result::Result<
                    ::scylla::statement::prepared::PreparedStatement,
                    $crate::cassandra::errors::CassandraStoreError
                > {
                    let sql_query = $crate::cassandra::macros::format_sql(
                        $sql,
                        keyspace,
                        &[$($arg),*]
                    );
                    $crate::cassandra::macros::prepare_statement(
                        session,
                        &sql_query,
                    ).await
                }
            }
        )*
    };
}
