use std::{
    any::{Any, TypeId},
    collections::HashMap,
    ops::Deref,
    sync::Arc,
};

use crate::step;

struct Entry {
    /// An `Arc<T>` boxed as `Any`, so `T` may be unsized (`dyn Trait`).
    value: Box<dyn Any + Send + Sync>,
    pure: bool,
}

/// Registered application clients, shared by every execution.
#[derive(Clone, Default)]
pub struct Deps {
    map: Arc<HashMap<TypeId, Entry>>,
}

#[derive(Default)]
pub struct DepsBuilder {
    map: HashMap<TypeId, Entry>,
}

impl DepsBuilder {
    /// Effectful dependency: only usable inside a step.
    pub fn insert<T: ?Sized + Send + Sync + 'static>(&mut self, v: Arc<T>) {
        self.put(v, false);
    }

    /// Deterministic, read-only dependency: usable anywhere, including the body.
    pub fn insert_pure<T: ?Sized + Send + Sync + 'static>(&mut self, v: Arc<T>) {
        self.put(v, true);
    }

    fn put<T: ?Sized + Send + Sync + 'static>(&mut self, v: Arc<T>, pure: bool) {
        self.map.insert(
            TypeId::of::<T>(),
            Entry {
                value: Box::new(v),
                pure,
            },
        );
    }

    pub fn build(self) -> Deps {
        Deps {
            map: Arc::new(self.map),
        }
    }
}

impl Deps {
    pub fn builder() -> DepsBuilder {
        DepsBuilder::default()
    }

    pub fn contains<T: ?Sized + 'static>(&self) -> bool {
        self.has(TypeId::of::<T>())
    }

    pub(crate) fn has(&self, id: TypeId) -> bool {
        self.map.contains_key(&id)
    }

    pub fn get<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Dep<T>> {
        let entry = self.map.get(&TypeId::of::<T>())?;
        let inner = entry.value.downcast_ref::<Arc<T>>()?.clone();
        Some(Dep {
            inner,
            pure: entry.pure,
        })
    }
}

/// A dependency handle. Effectful ones refuse to be dereferenced outside a step.
pub struct Dep<T: ?Sized> {
    inner: Arc<T>,
    pure: bool,
}

impl<T: ?Sized> Clone for Dep<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            pure: self.pure,
        }
    }
}

impl<T: ?Sized> Deref for Dep<T> {
    type Target = T;
    fn deref(&self) -> &T {
        if !self.pure && !step::in_step() {
            panic!(
                "dependency `{}` used outside a step: it would run again on every replay. \
                 Call it inside ctx.step(..), or register it with dep_pure() if it is deterministic",
                std::any::type_name::<T>()
            );
        }
        &self.inner
    }
}

/// Names a required dependency for `Worker::build()` validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DepKey {
    pub id: TypeId,
    pub name: &'static str,
}

impl DepKey {
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::step::{scope_blocking, test_info};

    trait Mailer: Send + Sync {
        fn send(&self) -> &'static str;
    }
    struct Smtp;
    impl Mailer for Smtp {
        fn send(&self) -> &'static str {
            "sent"
        }
    }
    struct Config(u32);

    fn deps() -> Deps {
        let mut b = Deps::builder();
        b.insert::<dyn Mailer>(Arc::new(Smtp));
        b.insert_pure(Arc::new(Config(7)));
        b.build()
    }

    #[test]
    fn resolves_trait_objects_and_concrete_types() {
        let d = deps();
        assert!(d.contains::<dyn Mailer>() && d.contains::<Config>());
        assert!(d.get::<String>().is_none());
        assert_eq!(d.get::<Config>().unwrap().0, 7);
    }

    #[test]
    fn effectful_dep_works_inside_a_step() {
        let m = deps().get::<dyn Mailer>().unwrap();
        let out = scope_blocking(test_info("r:s"), || m.send());
        assert_eq!(out, "sent");
    }

    #[test]
    fn effectful_dep_outside_a_step_panics_with_a_clear_message() {
        let m = deps().get::<dyn Mailer>().unwrap();
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| m.send())).unwrap_err();
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("used outside a step"), "{msg}");
        assert!(msg.contains("Mailer"), "{msg}");
        let _ = step::in_step();
    }

    #[test]
    fn pure_dep_is_usable_in_the_body() {
        assert_eq!(deps().get::<Config>().unwrap().0, 7);
    }
}
