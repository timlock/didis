use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::task::Poll;

pub async fn select<T>(futures: &mut [Pin<&mut impl Future<Output = T>>]) -> T {
    poll_fn(|cx| {
        for future in futures.iter_mut() {
            match future.as_mut().poll(cx) {
                Poll::Ready(result) => return Poll::Ready(result),
                Poll::Pending => {}
            };
        }

        Poll::Pending
    })
        .await
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub enum SelectResult<A, B> {
    A(A),
    B(B),
}
pub async fn select_pair<A: Future, B: Future>(a: A, b: B) -> SelectResult<A::Output, B::Output> {
    let mut first_pinned = pin!(a);
    let mut second_pinned = pin!(b);
    poll_fn(|cx| {
        match first_pinned.as_mut().poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectResult::A(result)),
            Poll::Pending => {}
        }

        match second_pinned.as_mut().poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectResult::B(result)),
            Poll::Pending => {}
        }

        Poll::Pending
    })
        .await
}

pub async fn select_without_drop<A: Future + Unpin, B: Future + Unpin>(
    mut a: A,
    mut b: B,
) -> SelectResult<(A::Output, B), (B::Output, A)> {
    let result = poll_fn(|cx| {
        match Pin::new(&mut a).poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectResult::A(result)),
            Poll::Pending => {}
        }

        match Pin::new(&mut b).poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectResult::B(result)),
            Poll::Pending => {}
        }

        Poll::Pending
    })
        .await;

    match result {
        SelectResult::A(result) => SelectResult::A((result, b)),
        SelectResult::B(result) => SelectResult::B((result, a)),
    }
}

pub enum SelectTripleResult<A, B, C> {
    A(A),
    B(B),
    C(C),
}

pub async fn select_triple<A, B, C>(
    a: impl Future<Output = A>,
    b: impl Future<Output = B>,
    c: impl Future<Output = C>,
) -> SelectTripleResult<A, B, C> {
    let mut a_pinned = pin!(a);
    let mut b_pinned = pin!(b);
    let mut c_pinned = pin!(c);
    poll_fn(|cx| {
        match a_pinned.as_mut().poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectTripleResult::A(result)),
            Poll::Pending => {}
        }

        match b_pinned.as_mut().poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectTripleResult::B(result)),
            Poll::Pending => {}
        }

        match c_pinned.as_mut().poll(cx) {
            Poll::Ready(result) => return Poll::Ready(SelectTripleResult::C(result)),
            Poll::Pending => {}
        }

        Poll::Pending
    })
        .await
}