#[cfg(shuttle)]
pub(crate) use shuttle::check_random;
#[cfg(shuttle)]
pub(crate) use shuttle::rand::thread_rng;
#[cfg(shuttle)]
#[allow(unused_imports)]
pub(crate) use shuttle::rand::Rng;
#[cfg(shuttle)]
pub(crate) use shuttle::rand::RngCore;
#[cfg(shuttle)]
pub(crate) use shuttle::sync::{Arc, Mutex, RwLock};
#[cfg(shuttle)]
pub(crate) use shuttle::thread;

#[cfg(not(shuttle))]
pub(crate) use rand::thread_rng;
#[cfg(not(shuttle))]
#[allow(unused_imports)]
pub(crate) use rand::Rng;
#[cfg(not(shuttle))]
pub(crate) use rand::RngCore;
#[cfg(not(shuttle))]
pub(crate) use std::sync::{Arc, Mutex, RwLock};
#[cfg(not(shuttle))]
pub(crate) use std::thread;

#[cfg(not(shuttle))]
pub(crate) fn check_random<F>(f: F, _iterations: usize)
where
    F: Fn() + Send + Sync + 'static,
{
    f()
}
