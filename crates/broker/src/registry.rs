//! Service registry: maps `service_name → ServiceEntry`.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokimo_bus_protocol::{BusFrame, MethodDecl};

/// A single registered service connection.
#[derive(Clone)]
pub struct ServiceEntry {
    /// Service name (redundant with the registry key but convenient).
    pub service: String,
    /// Monotonically increasing id, incremented on each reconnect.
    pub generation: u64,
    /// Outbound frame channel to this app's writer task.
    pub tx: mpsc::UnboundedSender<BusFrame>,
    /// Methods the app declared in its `Hello`.
    pub methods: Arc<Vec<MethodDecl>>,
    /// Process id, informational.
    pub pid: u32,
}

/// In-memory registry. Concurrent because HTTP handlers call into it without
/// blocking the broker's accept loop.
#[derive(Default)]
pub struct Registry {
    entries: DashMap<String, ServiceEntry>,
    next_gen: parking_lot::Mutex<u64>,
}

impl Registry {
    /// Register or replace a service entry. Returns the new generation.
    pub fn insert(&self, mut entry: ServiceEntry) -> u64 {
        let gen = {
            let mut g = self.next_gen.lock();
            *g += 1;
            *g
        };
        entry.generation = gen;
        self.entries.insert(entry.service.clone(), entry);
        gen
    }

    /// Look up an entry by service name.
    pub fn get(&self, service: &str) -> Option<ServiceEntry> {
        self.entries.get(service).map(|e| e.clone())
    }

    /// Remove an entry if its generation still matches — prevents a stale
    /// reader task from evicting a freshly-registered successor.
    pub fn remove_if(&self, service: &str, generation: u64) -> bool {
        let mut removed = false;
        self.entries.remove_if(service, |_, v| {
            if v.generation == generation {
                removed = true;
                true
            } else {
                false
            }
        });
        removed
    }

    /// Snapshot of the full registry for diagnostics.
    pub fn list(&self) -> Vec<ServiceEntry> {
        self.entries.iter().map(|e| e.value().clone()).collect()
    }
}
