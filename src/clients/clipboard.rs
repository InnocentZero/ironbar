use super::wayland::{self, ClipboardItem};
use base64::Engine;
use crate::channels::AsyncSenderExt;
use crate::{arc_mut, lock, register_client, spawn};
use indexmap::IndexMap;
use indexmap::map::Iter;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, trace};

#[cfg(any(feature = "ipc", feature = "cairo"))]
use crate::ironvar::NamespaceTrait;
#[cfg(any(feature = "ipc", feature = "cairo"))]
use crate::clients::wayland::ClipboardValue;

#[derive(Debug)]
pub enum ClipboardEvent {
    Add(ClipboardItem),
    Remove(usize),
    Activate(usize),
}

type EventSender = mpsc::Sender<ClipboardEvent>;

/// Clipboard client singleton,
/// to ensure bars don't duplicate requests to the compositor.
#[derive(Debug, Clone)]
pub struct Client {
    wayland: Arc<wayland::Client>,

    senders: Arc<Mutex<Vec<(EventSender, usize)>>>,
    cache: Arc<Mutex<ClipboardCache>>,
    current_id: Arc<Mutex<Option<usize>>>,
}

impl Client {
    pub(crate) fn new(wl: Arc<wayland::Client>) -> Self {
        trace!("Initializing clipboard client");

        let senders = arc_mut!(Vec::<(EventSender, usize)>::new());

        let cache = arc_mut!(ClipboardCache::new());
        let current_id = arc_mut!(None);

        {
            let senders = senders.clone();
            let cache = cache.clone();
            let current_id = current_id.clone();
            let wl = wl.clone();

            spawn(async move {
                let item = wl.clipboard_item();
                let mut rx = wl.subscribe_clipboard();

                if let Some(item) = item {
                    let senders = lock!(senders);
                    let iter = senders.iter();
                    for (tx, _) in iter {
                        tx.send_spawn(ClipboardEvent::Add(item.clone()));
                    }

                    lock!(current_id).replace(item.id);
                    lock!(cache).insert(item, senders.len());
                }

                while let Ok(item) = rx.recv().await {
                    debug!("Received clipboard item (ID: {})", item.id);

                    let (existing_id, cache_size) = {
                        let cache = lock!(cache);
                        (cache.contains(&item), cache.len())
                    };

                    existing_id.map_or_else(
                        || {
                            lock!(current_id).replace(item.id);
                            {
                                let mut cache = lock!(cache);
                                let senders = lock!(senders);
                                cache.insert(item.clone(), senders.len());
                            }
                            let senders = lock!(senders);
                            let iter = senders.iter();
                            for (tx, sender_cache_size) in iter {
                                if cache_size == *sender_cache_size {
                                    let removed_id = lock!(cache)
                                        .remove_ref_first()
                                        .expect("Clipboard cache unexpectedly empty");

                                    tx.send_spawn(ClipboardEvent::Remove(removed_id));
                                }
                                tx.send_spawn(ClipboardEvent::Add(item.clone()));
                            }
                        },
                        |existing_id| {
                            lock!(current_id).replace(existing_id);
                            let senders = lock!(senders);
                            let iter = senders.iter();
                            for (tx, _) in iter {
                                tx.send_spawn(ClipboardEvent::Activate(existing_id));
                            }
                        },
                    );
                }
            });
        }

        Self {
            wayland: wl,
            senders,
            cache,
            current_id,
        }
    }

    pub fn subscribe(&self, cache_size: usize) -> mpsc::Receiver<ClipboardEvent> {
        let (tx, rx) = mpsc::channel(16);

        {
            let cache = lock!(self.cache);

            let iter = cache.iter();
            for (_, (item, _)) in iter {
                tx.send_spawn(ClipboardEvent::Add(item.clone()));
            }
        }

        lock!(self.senders).push((tx, cache_size));

        rx
    }

    pub fn copy(&self, id: usize) {
        debug!("Copying item with id {id}");

        let item = {
            let cache = lock!(self.cache);
            cache.get(id)
        };

        if let Some(item) = item {
            self.wayland.copy_to_clipboard(item);
        }

        lock!(self.current_id).replace(id);
        let senders = lock!(self.senders);
        let iter = senders.iter();
        for (tx, _) in iter {
            tx.send_spawn(ClipboardEvent::Activate(id));
        }
    }

    pub fn remove(&self, id: usize) {
        lock!(self.cache).remove(id);
        let mut current_id = lock!(self.current_id);
        if current_id.is_some_and(|current_id| current_id == id) {
            current_id.take();
        }

        let senders = lock!(self.senders);
        let iter = senders.iter();
        for (tx, _) in iter {
            tx.send_spawn(ClipboardEvent::Remove(id));
        }
    }

    #[cfg(any(feature = "ipc", feature = "cairo"))]
    fn current_item(&self) -> Option<ClipboardItem> {
        let current_id = *lock!(self.current_id);
        current_id.and_then(|id| lock!(self.cache).get(id))
    }
}

/// Shared clipboard item cache.
///
/// Items are stored with a number of references,
/// allowing different consumers to 'remove' cached items
/// at different times.
#[derive(Debug)]
struct ClipboardCache {
    cache: IndexMap<usize, (ClipboardItem, usize)>,
}

impl ClipboardCache {
    /// Creates a new empty cache.
    fn new() -> Self {
        Self {
            cache: IndexMap::new(),
        }
    }

    /// Gets the entry with key `id` from the cache.
    fn get(&self, id: usize) -> Option<ClipboardItem> {
        self.cache.get(&id).map(|(item, _)| item).cloned()
    }

    /// Inserts an entry with `ref_count` initial references.
    fn insert(&mut self, item: ClipboardItem, ref_count: usize) -> Option<ClipboardItem> {
        self.cache
            .insert(item.id, (item, ref_count))
            .map(|(item, _)| item)
    }

    /// Removes the entry with key `id`.
    /// This ignores references.
    fn remove(&mut self, id: usize) -> Option<ClipboardItem> {
        self.cache.shift_remove(&id).map(|(item, _)| item)
    }

    /// Removes a reference to the entry with key `id`.
    ///
    /// If the reference count reaches zero, the entry
    /// is removed from the cache.
    fn remove_ref(&mut self, id: usize) {
        if let Some(entry) = self.cache.get_mut(&id) {
            entry.1 -= 1;

            if entry.1 == 0 {
                self.cache.shift_remove(&id);
            }
        }
    }

    /// Removes a reference to the first entry.
    ///
    /// If the reference count reaches zero, the entry
    /// is removed from the cache.
    fn remove_ref_first(&mut self) -> Option<usize> {
        if let Some((id, _)) = self.cache.first() {
            let id = *id;
            self.remove_ref(id);
            Some(id)
        } else {
            None
        }
    }

    /// Checks if an item with matching mime type and value
    /// already exists in the cache.
    fn contains(&self, item: &ClipboardItem) -> Option<usize> {
        self.cache.values().find_map(|(it, _)| {
            if it.mime_type == item.mime_type && it.value == item.value {
                Some(it.id)
            } else {
                None
            }
        })
    }

    /// Gets the current number of items in the cache.
    fn len(&self) -> usize {
        self.cache.len()
    }

    fn iter(&self) -> Iter<'_, usize, (ClipboardItem, usize)> {
        self.cache.iter()
    }
}

#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_NAMESPACE: &str = "current";
#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_KEY_ID: &str = "id";
#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_KEY_MIME_TYPE: &str = "mime_type";
#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_KEY_TYPE: &str = "type";
#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_KEY_DATA: &str = "data";
#[cfg(any(feature = "ipc", feature = "cairo"))]
const CURRENT_KEY_SIZE: &str = "size";

#[cfg(any(feature = "ipc", feature = "cairo"))]
#[derive(Debug)]
struct CurrentItem {
    client: Arc<Client>,
}

#[cfg(any(feature = "ipc", feature = "cairo"))]
impl crate::ironvar::Namespace for CurrentItem {
    fn get(&self, key: &str) -> Option<String> {
        let item = self.client.current_item()?;

        match key {
            CURRENT_KEY_ID => Some(item.id.to_string()),
            CURRENT_KEY_MIME_TYPE => Some(item.mime_type.to_string()),
            CURRENT_KEY_TYPE => Some(match item.value.as_ref() {
                ClipboardValue::Text(_) => "text".to_string(),
                ClipboardValue::Image(_) => "image".to_string(),
                ClipboardValue::Other => "other".to_string(),
            }),
            CURRENT_KEY_DATA => match item.value.as_ref() {
                ClipboardValue::Text(value) => Some(value.clone()),
                ClipboardValue::Image(bytes) => {
                    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
                }
                ClipboardValue::Other => None,
            },
            CURRENT_KEY_SIZE => Some(match item.value.as_ref() {
                ClipboardValue::Text(value) => value.len(),
                ClipboardValue::Image(bytes) => bytes.len(),
                ClipboardValue::Other => 0,
            }
            .to_string()),
            _ => None,
        }
    }

    fn list(&self) -> Vec<String> {
        [
            CURRENT_KEY_ID,
            CURRENT_KEY_MIME_TYPE,
            CURRENT_KEY_TYPE,
            CURRENT_KEY_DATA,
            CURRENT_KEY_SIZE,
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect()
    }

    fn namespaces(&self) -> Vec<String> {
        vec![]
    }

    fn get_namespace(&self, _key: &str) -> Option<NamespaceTrait> {
        None
    }
}

#[cfg(any(feature = "ipc", feature = "cairo"))]
impl crate::ironvar::Namespace for Client {
    fn get(&self, _key: &str) -> Option<String> {
        None
    }

    fn list(&self) -> Vec<String> {
        Vec::new()
    }

    fn namespaces(&self) -> Vec<String> {
        vec![CURRENT_NAMESPACE.to_string()]
    }

    fn get_namespace(&self, key: &str) -> Option<NamespaceTrait> {
        match key {
            CURRENT_NAMESPACE => Some(Arc::new(CurrentItem {
                client: Arc::new(self.clone()),
            })),
            _ => None,
        }
    }
}

register_client!(Client, clipboard);
