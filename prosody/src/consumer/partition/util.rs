use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;

#[pin_project]
pub struct WithValue<T, F> {
    value: T,
    #[pin]
    future: F,
}

impl<T, F> WithValue<T, F> {
    pub fn new(value: T, future: F) -> Self {
        WithValue { value, future }
    }
}

impl<T, F> Future for WithValue<T, F>
where
    T: Clone,
    F: Future,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut projection = self.project();
        projection
            .future
            .as_mut()
            .poll(cx)
            .map(|_| projection.value.clone())
    }
}
