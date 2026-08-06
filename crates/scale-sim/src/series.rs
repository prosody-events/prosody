use std::marker::PhantomData;

use crate::PlantError;

const SERIES_COUNT_MAX: usize = u128::BITS as usize;

/// A scalar value that one simulation series can retain.
pub trait SeriesValue: Copy {
    /// Converts the value to an exact trace cell.
    fn into_cell(self) -> SeriesCell;

    /// Restores the value from an exact trace cell.
    fn from_cell(cell: SeriesCell) -> Option<Self>;
}

/// One exact scalar cell in a calculated series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SeriesCell {
    /// Floating-point value.
    Float(f64),
    /// Unsigned 32-bit value.
    Unsigned32(u32),
    /// Unsigned 64-bit value.
    Unsigned64(u64),
    /// Boolean value.
    Boolean(bool),
}

impl SeriesValue for f64 {
    fn into_cell(self) -> SeriesCell {
        SeriesCell::Float(self)
    }

    fn from_cell(cell: SeriesCell) -> Option<Self> {
        match cell {
            SeriesCell::Float(value) => Some(value),
            _ => None,
        }
    }
}

impl SeriesValue for u32 {
    fn into_cell(self) -> SeriesCell {
        SeriesCell::Unsigned32(self)
    }

    fn from_cell(cell: SeriesCell) -> Option<Self> {
        match cell {
            SeriesCell::Unsigned32(value) => Some(value),
            _ => None,
        }
    }
}

impl SeriesValue for u64 {
    fn into_cell(self) -> SeriesCell {
        SeriesCell::Unsigned64(self)
    }

    fn from_cell(cell: SeriesCell) -> Option<Self> {
        match cell {
            SeriesCell::Unsigned64(value) => Some(value),
            _ => None,
        }
    }
}

impl SeriesValue for bool {
    fn into_cell(self) -> SeriesCell {
        SeriesCell::Boolean(self)
    }

    fn from_cell(cell: SeriesCell) -> Option<Self> {
        match cell {
            SeriesCell::Boolean(value) => Some(value),
            _ => None,
        }
    }
}

/// Typed column identity for one calculated series.
#[derive(Clone, Copy)]
pub struct SeriesKey<Value> {
    pub(crate) column: usize,
    marker: PhantomData<Value>,
}

impl<Value> SeriesKey<Value> {
    /// Constructs a key for one graph column.
    #[must_use]
    pub(crate) const fn new(column: usize) -> Self {
        Self {
            column,
            marker: PhantomData,
        }
    }

    pub(crate) fn for_name(names: &[&str], name: &str) -> Result<Self, PlantError> {
        let Some(column) = names.iter().position(|candidate| *candidate == name) else {
            return Err(PlantError::PlatformLimit);
        };
        Ok(Self::new(column))
    }
}

/// Display unit for one calculated series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeriesUnit {
    /// Dimensionless count.
    Count,
    /// Boolean condition.
    Boolean,
    /// Time in microseconds.
    Microseconds,
    /// Replica count.
    Replicas,
}

/// Plot role for one calculated series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeriesRole {
    /// External workload or plant input.
    Input,
    /// Calculated internal state.
    State,
    /// Control action sent to the plant.
    Action,
}

/// Stable display metadata for one calculated series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeriesMetadata {
    /// Stable program name.
    pub name: &'static str,
    /// Short plot label.
    pub label: &'static str,
    /// Display unit.
    pub unit: SeriesUnit,
    /// Plot role.
    pub role: SeriesRole,
}

/// Context supplied to every function in one graph evaluation.
#[derive(Clone, Copy)]
pub struct SeriesContext<'a, Frame> {
    /// Current simulator frame.
    pub frame: Frame,
    /// Prior calculated graph rows.
    pub history: SeriesHistoryView<'a>,
}

/// One typed scalar function with explicit upstream dependencies.
pub trait SeriesFunction<Frame, Dependencies> {
    /// Calculated scalar type.
    type Output: SeriesValue;

    /// Calculates one value from the frame, history, and dependencies.
    fn calculate(
        &self,
        context: SeriesContext<'_, Frame>,
        dependencies: Dependencies,
    ) -> Self::Output;
}

/// One graph output function with explicit upstream dependencies.
pub trait OutputFunction<Frame, Dependencies> {
    /// Graph result type.
    type Output;

    /// Builds the graph result from the frame, history, and dependencies.
    fn calculate(
        &self,
        context: SeriesContext<'_, Frame>,
        dependencies: Dependencies,
    ) -> Self::Output;
}

/// Fixed-capacity row-major history for one calculation graph.
pub struct SeriesHistory {
    metadata: &'static [SeriesMetadata],
    at_micros: Vec<u64>,
    values: Vec<SeriesCell>,
    cursor: usize,
    length: usize,
}

/// Access to the automatically recorded values of one declared graph.
pub trait RecordedSeries {
    /// Returns the retained graph history.
    fn series_history(&self) -> &SeriesHistory;

    /// Consumes the graph and returns its retained history.
    fn into_series_history(self) -> SeriesHistory;
}

/// Returns true when every current-time edge forms one bounded DAG.
///
/// Prior-time edges do not belong to this graph. They read completed rows.
pub(crate) const fn series_graph_is_acyclic(names: &[&str], edges: &[(&str, &str)]) -> bool {
    if names.is_empty() || names.len() > SERIES_COUNT_MAX {
        return false;
    }
    let mut resolved = 0_u128;
    let complete = if names.len() == SERIES_COUNT_MAX {
        u128::MAX
    } else {
        (1_u128 << names.len()) - 1
    };
    while resolved != complete {
        let before = resolved;
        let mut node = 0_usize;
        while node < names.len() {
            if resolved & (1_u128 << node) == 0
                && dependencies_resolved(names, edges, names[node], resolved)
            {
                resolved |= 1_u128 << node;
            }
            node += 1;
        }
        if resolved == before {
            return false;
        }
    }
    true
}

const fn dependencies_resolved(
    names: &[&str],
    edges: &[(&str, &str)],
    node: &str,
    resolved: u128,
) -> bool {
    let mut edge = 0_usize;
    while edge < edges.len() {
        if string_eq(edges[edge].0, node) {
            let Some(dependency) = name_index(names, edges[edge].1) else {
                return false;
            };
            if resolved & (1_u128 << dependency) == 0 {
                return false;
            }
        }
        edge += 1;
    }
    true
}

pub(crate) const fn name_index(names: &[&str], needle: &str) -> Option<usize> {
    let mut index = 0_usize;
    while index < names.len() {
        if string_eq(names[index], needle) {
            return Some(index);
        }
        index += 1;
    }
    None
}

const fn string_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0_usize;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Read-only typed access to prior graph rows.
#[derive(Clone, Copy)]
pub struct SeriesHistoryView<'a> {
    history: &'a SeriesHistory,
}

impl SeriesHistory {
    /// Allocates one bounded table for the supplied columns.
    ///
    /// # Errors
    ///
    /// Returns an error when a bound is zero or exceeds this platform.
    pub fn new(
        metadata: &'static [SeriesMetadata],
        row_count_max: u32,
    ) -> Result<Self, PlantError> {
        if metadata.is_empty() {
            return Err(PlantError::ZeroBound {
                name: "series_column_count",
            });
        }
        let row_count = usize::try_from(row_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if row_count == 0 {
            return Err(PlantError::ZeroBound {
                name: "series_history_count_max",
            });
        }
        let value_count = row_count
            .checked_mul(metadata.len())
            .ok_or(PlantError::PlatformLimit)?;
        Ok(Self {
            metadata,
            at_micros: vec![0; row_count],
            values: vec![SeriesCell::Float(0.0_f64); value_count],
            cursor: 0,
            length: 0,
        })
    }

    /// Returns a read-only view of retained rows.
    #[must_use]
    pub const fn view(&self) -> SeriesHistoryView<'_> {
        SeriesHistoryView { history: self }
    }

    /// Returns stable column names in declaration order.
    #[must_use]
    pub fn names(&self) -> impl ExactSizeIterator<Item = &'static str> + '_ {
        self.metadata.iter().map(|metadata| metadata.name)
    }

    /// Returns stable metadata in declaration order.
    #[must_use]
    pub const fn metadata(&self) -> &'static [SeriesMetadata] {
        self.metadata
    }

    /// Returns the retained row count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    /// Returns true when the history has no row.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns virtual time for one oldest-first row index.
    #[must_use]
    pub fn at_micros(&self, row: usize) -> Option<u64> {
        self.row(row).map(|index| self.at_micros[index])
    }

    /// Returns one exact cell by series name and oldest-first row index.
    #[must_use]
    pub fn cell(&self, name: &str, row: usize) -> Option<SeriesCell> {
        let column = self
            .metadata
            .iter()
            .position(|metadata| metadata.name == name)?;
        let row = self.row(row)?;
        Some(self.values[row * self.metadata.len() + column])
    }

    pub(crate) fn push_indexed(&mut self, at_micros: u64, values: &[(usize, SeriesCell)]) {
        let row = self.cursor;
        let start = row * self.metadata.len();
        self.at_micros[row] = at_micros;
        for &(column, value) in values {
            self.values[start + column] = value;
        }
        self.cursor = (self.cursor + 1) % self.at_micros.len();
        self.length = (self.length + 1).min(self.at_micros.len());
    }

    fn row(&self, oldest_first: usize) -> Option<usize> {
        if oldest_first >= self.length {
            return None;
        }
        let capacity = self.at_micros.len();
        let oldest = (self.cursor + capacity - self.length) % capacity;
        Some((oldest + oldest_first) % capacity)
    }
}

impl SeriesHistoryView<'_> {
    /// Returns the retained row count.
    #[must_use]
    pub const fn len(self) -> usize {
        self.history.length
    }

    /// Returns true when the history has no row.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.history.length == 0
    }

    /// Returns virtual time for one newest-first row offset.
    #[must_use]
    pub fn at_micros(self, steps_back: usize) -> Option<u64> {
        self.row(steps_back).map(|row| self.history.at_micros[row])
    }

    /// Returns one typed value for a newest-first row offset.
    #[must_use]
    pub fn get<Value: SeriesValue>(
        self,
        key: SeriesKey<Value>,
        steps_back: usize,
    ) -> Option<Value> {
        let row = self.row(steps_back)?;
        let index = row * self.history.metadata.len() + key.column;
        Value::from_cell(self.history.values[index])
    }

    fn row(self, steps_back: usize) -> Option<usize> {
        if steps_back >= self.history.length {
            return None;
        }
        let capacity = self.history.at_micros.len();
        Some((self.history.cursor + capacity - 1 - steps_back) % capacity)
    }
}

macro_rules! series_graph {
    (
        $(#[$meta:meta])*
        $visibility:vis struct $graph:ident($frame:ty) with (
            $($parameter:ident: $parameter_type:ty),* $(,)?
        ) {
            $(
                series $field:ident: $value:ty [$label:literal, $unit:ident, $role:ident] =
                    $function:ident $initializer:tt =>
                    ($($dependency:ident),* $(,)?)
                    $(previous ($($previous:ident),* $(,)?))?;
            )+
            output $output_field:ident: $output:ty = $output_function:ident $output_initializer:tt =>
                ($($output_dependency:ident),* $(,)?);
        }
    ) => {
        paste::paste! {
        const _: [(); 1] = [(); $crate::series::series_graph_is_acyclic(
            &[$(stringify!($field)),+],
            &[$($((stringify!($field), stringify!($dependency)),)*)+],
        ) as usize];

        $(#[$meta])*
        $visibility struct $graph {
            $(
                $field: $function,
            )+
            $output_field: $output_function,
            keys: [<$graph Keys>],
            history: $crate::series::SeriesHistory,
        }

        struct [<$graph Keys>] {
            $($field: $crate::series::SeriesKey<$value>,)+
        }

        struct [<$graph Evaluation>] {
            $($field: Option<$value>,)+
        }

        impl [<$graph Evaluation>] {
            const fn new() -> Self {
                Self {
                    $($field: None,)+
                }
            }

            $(
                fn $field(
                    &mut self,
                    graph: &$graph,
                    context: $crate::series::SeriesContext<'_, $frame>,
                ) -> $value {
                    if let Some(value) = self.$field {
                        return value;
                    }
                    let dependencies = (
                        $(self.$dependency(graph, context),)*
                        $($(context.history.get(
                            graph.keys.$previous,
                            0,
                        ),)*)?
                    );
                    let value = $crate::series::SeriesFunction::calculate(
                        &graph.$field,
                        context,
                        dependencies,
                    );
                    self.$field = Some(value);
                    value
                }
            )+
        }

        impl $graph {
            $visibility fn new(
                $($parameter: $parameter_type,)*
                history_count_max: u32,
            ) -> Result<Self, $crate::PlantError> {
                const METADATA: &[$crate::series::SeriesMetadata] = &[$(
                    $crate::series::SeriesMetadata {
                        name: stringify!($field),
                        label: $label,
                        unit: $crate::series::SeriesUnit::$unit,
                        role: $crate::series::SeriesRole::$role,
                    },
                )+];
                const NAMES: &[&str] = &[$(stringify!($field)),+];
                Ok(Self {
                    $($field: $function $initializer,)+
                    $output_field: $output_function $output_initializer,
                    keys: [<$graph Keys>] {
                        $($field: $crate::series::SeriesKey::for_name(
                            NAMES,
                            stringify!($field),
                        )?,)+
                    },
                    history: $crate::series::SeriesHistory::new(METADATA, history_count_max)?,
                })
            }

            $visibility fn evaluate(
                &mut self,
                at_micros: u64,
                frame: $frame,
            ) -> $output {
                let context = $crate::series::SeriesContext {
                    frame,
                    history: self.history.view(),
                };
                let mut evaluation = [<$graph Evaluation>]::new();
                $(
                    let $field: $value = evaluation.$field(self, context);
                )+
                let output: $output = $crate::series::OutputFunction::calculate(
                    &self.$output_field,
                    context,
                    ($($output_dependency,)*),
                );
                self.history.push_indexed(
                    at_micros,
                    &[$((
                        self.keys.$field.column,
                        $crate::series::SeriesValue::into_cell($field),
                    )),+],
                );
                output
            }

        }

        impl $crate::series::RecordedSeries for $graph {
            fn series_history(&self) -> &$crate::series::SeriesHistory {
                &self.history
            }

            fn into_series_history(self) -> $crate::series::SeriesHistory {
                self.history
            }
        }
        }
    };
}

pub(crate) use series_graph;
