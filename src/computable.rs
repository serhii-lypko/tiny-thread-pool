/// A concurrent data structure to measure.
pub trait Computable {
    /// The value `curr` reports — typically the final result of a batch
    /// (e.g. the counter total), used to assert correctness in tests.
    type Inner: std::fmt::Debug;

    /// Perform one unit of work against the shared state.
    fn compute_step(&self) -> bool;

    /// Clear the shared state back to its starting point.
    fn reset(&self);

    /// Read the current value of the shared state.
    fn curr(&self) -> Self::Inner;
}
