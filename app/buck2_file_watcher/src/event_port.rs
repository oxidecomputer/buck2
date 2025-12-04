//! illumos event port based file watcher

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;
use std::panic;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_common::file_ops::dice::FileChangeTracker;
use buck2_common::ignores::ignore_set::IgnoreSet;
use buck2_common::invocation_paths::InvocationPaths;
use buck2_core::cells::CellResolver;
use buck2_core::cells::cell_path::CellPath;
use buck2_core::cells::name::CellName;
use buck2_core::fs::project::ProjectRoot;
use buck2_data::FileWatcherEventType;
use buck2_data::FileWatcherKind;
use buck2_error::BuckErrorContext;
use buck2_error::buck2_error;
use buck2_events::dispatch::span_async;
use buck2_fs::paths::abs_norm_path::AbsNormPath;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use dice::DiceTransactionUpdater;
use dupe::Dupe;
use event_ports_sys::EventPort;
use event_ports_sys::EventSource;
use event_ports_sys::FileEvents;
use event_ports_sys::FileObj;
use starlark_map::ordered_set::OrderedSet;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::file_watcher::FileWatcher;
use crate::mergebase::Mergebase;
use crate::stats::FileWatcherStats;

/// Event types that we track for file watching
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EventPortEventType {
    Created,
    Modified,
    Deleted,
    Renamed,
}

/// Buffer containing the events that have happened since we last got a sync message.
/// Used to dedupe events, since event ports can send multiple events for the same file.
#[derive(Allocative)]
struct EventPortFileData {
    ignored: u64,
    #[allocative(skip)]
    events: OrderedSet<(CellPath, EventPortEventType, bool)>, // (path, event_type, is_dir)
}

impl EventPortFileData {
    fn new() -> Self {
        Self {
            ignored: 0,
            events: OrderedSet::new(),
        }
    }

    fn process(
        &mut self,
        path: Utf8PathBuf,
        event_type: EventPortEventType,
        is_dir: bool,
        root: &ProjectRoot,
        cells: &CellResolver,
        ignore_specs: &HashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<()> {
        // Convert to relative path from project root
        let abs_path = AbsNormPath::new(path.as_std_path())
            .buck_error_context("Invalid absolute path from event port")?;
        let rel_path = root.relativize(abs_path)?;

        // Ignore buck-out directories (same as notify watcher)
        if rel_path.starts_with(InvocationPaths::buck_out_dir_prefix()) {
            debug!("Filtered (buck-out): {} {:?}", rel_path, event_type);
            return Ok(());
        }

        // Ignore temporary files (editor backups, buck temp files, etc.)
        if let Some(file_name) = path.file_name() {
            let name = file_name;
            // Skip common temporary file patterns
            if name.ends_with(".bck")
                || name.ends_with(".tmp")
                || name.ends_with('~')
                || name.starts_with(".#")
                || name.starts_with("#")
                || (name.starts_with('.') && name.ends_with(".swp"))
            {
                debug!("Filtered (temp file): {} {:?}", rel_path, event_type);
                self.ignored += 1;
                return Ok(());
            }
        }

        let cell_path = cells.get_cell_path(&rel_path);
        let ignore = ignore_specs
            .get(&cell_path.cell())
            .is_some_and(|ignore| ignore.is_match(cell_path.path()));

        info!(
            "FileWatcher: {:?} {:?} (is_dir = {}, ignore = {})",
            rel_path, event_type, is_dir, ignore
        );

        if ignore {
            info!("Filtered (ignore_specs): {} {:?} (cell={})", rel_path, event_type, cell_path.cell());
            self.ignored += 1;
        } else {
            info!("Buffering event: {} {:?} (is_dir={})", rel_path, event_type, is_dir);
            self.events.insert((cell_path, event_type, is_dir));
        }

        Ok(())
    }

    fn sync(self) -> (buck2_data::FileWatcherStats, FileChangeTracker) {
        let mut changed = FileChangeTracker::new();
        let mut stats = FileWatcherStats::new(Default::default(), self.events.len());
        stats.add_ignored(self.ignored);

        for (cell_path, event_type, is_dir) in self.events {
            let cell_path_str = cell_path.to_string();
            match event_type {
                EventPortEventType::Created => {
                    if is_dir {
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::Directory,
                        );
                    } else {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::File,
                        );
                    }
                }
                EventPortEventType::Modified => {
                    if is_dir {
                        // Directory modifications are handled via rescan_directory
                        // which generates explicit Created/Deleted events for children.
                        // Don't report directory modifications directly to avoid duplicate events.
                        // Just track it in stats.
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Modify,
                            FileWatcherKind::Directory,
                        );
                    } else {
                        changed.file_contents_changed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Modify,
                            FileWatcherKind::File,
                        );
                    }
                }
                EventPortEventType::Deleted => {
                    if is_dir {
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Delete,
                            FileWatcherKind::Directory,
                        );
                    } else {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Delete,
                            FileWatcherKind::File,
                        );
                    }
                }
                EventPortEventType::Renamed => {
                    // Treat rename as both a delete and create
                    if is_dir {
                        changed.dir_added_or_removed(cell_path.clone());
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Delete,
                            FileWatcherKind::Directory,
                        );
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::Directory,
                        );
                    } else {
                        changed.file_added_or_removed(cell_path.clone());
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Delete,
                            FileWatcherKind::File,
                        );
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::File,
                        );
                    }
                }
            }
        }

        (stats.finish(), changed)
    }
}

#[derive(Allocative)]
pub struct EventPortFileWatcher {
    data: Arc<Mutex<buck2_error::Result<EventPortFileData>>>,
    #[allocative(skip)]
    _handle: Option<thread::JoinHandle<()>>,
}

impl EventPortFileWatcher {
    pub fn new(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: HashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<Self> {
        let data = Arc::new(Mutex::new(Ok(EventPortFileData::new())));
        let data2 = data.dupe();
        let root2 = root.dupe();

        let root_path = root.root().as_path().to_path_buf();

        // Spawn background thread to monitor file system events
        let handle = thread::Builder::new()
            .name("event-port-watcher".to_owned())
            .spawn(move || {
                // Catch any panics to prevent daemon crash
                let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    run_event_port_loop(&root_path, data2, root2, cells, ignore_specs)
                }));

                match result {
                    Ok(Ok(())) => {
                        info!("Event port watcher thread exited normally");
                    }
                    Ok(Err(e)) => {
                        warn!("Event port watcher thread failed: {:?}", e);
                    }
                    Err(panic_err) => {
                        warn!("Event port watcher thread panicked: {:?}", panic_err);
                    }
                }
            })
            .map_err(|e| buck2_error!(
                buck2_error::ErrorTag::Environment,
                "Failed to spawn event port watcher thread: {}",
                e
            ))?;

        Ok(Self {
            data,
            _handle: Some(handle),
        })
    }

    fn sync2(
        &self,
        mut dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, DiceTransactionUpdater)> {
        let mut guard = self.data.lock()
            .map_err(|e| buck2_error!(
                buck2_error::ErrorTag::Environment,
                "Event port watcher lock poisoned: {}",
                e
            ))?;
        let old = mem::replace(&mut *guard, Ok(EventPortFileData::new()));
        let (stats, changes) = old?.sync();
        changes.write_to_dice(&mut dice)?;
        Ok((stats, dice))
    }
}

#[async_trait]
impl FileWatcher for EventPortFileWatcher {
    async fn sync(
        &self,
        dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(DiceTransactionUpdater, Mergebase)> {
        span_async(
            buck2_data::FileWatcherStart {
                provider: buck2_data::FileWatcherProvider::EventPorts as i32,
            },
            async {
                let (stats, res) = match self.sync2(dice) {
                    Ok((stats, dice)) => {
                        let mergebase = Mergebase(Arc::new(stats.branched_from_revision.clone()));
                        ((Some(stats)), Ok((dice, mergebase)))
                    }
                    Err(e) => (None, Err(e)),
                };
                (res, buck2_data::FileWatcherEnd { stats })
            },
        )
        .await
    }
}

/// Compute a cookie (hash) for a path to use as a user-provided identifier
fn path_to_cookie(path: &Utf8Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

/// Main event loop that monitors file system changes via event ports
fn run_event_port_loop(
    root_path: &std::path::Path,
    data: Arc<Mutex<buck2_error::Result<EventPortFileData>>>,
    root: ProjectRoot,
    cells: CellResolver,
    ignore_specs: HashMap<CellName, IgnoreSet>,
) -> anyhow::Result<()> {
    let ep = EventPort::new()
        .map_err(|e| anyhow::anyhow!("Failed to create event port: {}", e))?;

    // Use Box to keep FileObjs at stable heap locations
    // This prevents them from moving when the HashMap reorganizes
    let mut watchers: HashMap<Utf8PathBuf, Box<FileObj>> = HashMap::new();

    // Map from cookie (path hash) to path - used to identify events
    // We can't use pointer comparison because the kernel stores a pointer to internal
    // file_obj_t structure, not the FileObj itself
    let mut cookie_to_path: HashMap<u64, Utf8PathBuf> = HashMap::new();

    // Events we want to monitor (exclude ACCESS to reduce noise)
    let flags = FileEvents::MODIFIED | FileEvents::ATTRIB | FileEvents::TRUNC;

    // Initial recursive setup
    let root_utf8 = Utf8Path::from_path(root_path)
        .ok_or_else(|| anyhow::anyhow!("Root path is not valid UTF-8: {:?}", root_path))?;

    info!("Event port watcher: Setting up recursive watchers for {}", root_utf8);

    if let Err(e) = add_watchers_recursive(&ep, root_utf8, &mut watchers, &mut cookie_to_path, flags) {
        warn!("Failed to set up initial watchers: {}", e);
        return Err(anyhow::anyhow!("Failed to set up initial watchers: {}", e));
    }

    info!("Event port watcher monitoring {} paths", watchers.len());

    // Log sample of watched paths for debugging
    let sample_size = 20.min(watchers.len());
    info!("Sample of watched paths ({} of {}):", sample_size, watchers.len());
    for (i, path) in watchers.keys().take(sample_size).enumerate() {
        info!("  [{}] {}", i, path);
    }
    if watchers.len() > sample_size {
        info!("  ... and {} more", watchers.len() - sample_size);
    }

    if watchers.is_empty() {
        return Err(anyhow::anyhow!("No paths were successfully watched"));
    }

    info!("Event port watcher: Event loop ready, waiting for events");

    let mut event_count = 0u64;

    loop {
        event_count += 1;
        if event_count % 100 == 0 {
            debug!("Processed {} events", event_count);
        }

        // Use blocking mode - the timeout might cause EINVAL errors
        let event = match ep.get(None) {
            Ok(event) => event,
            Err(e) => {
                // Check error type
                if let Some(err) = e.0.raw_os_error() {
                    // EINTR = 4
                    if err == libc::EINTR {
                        // Interrupted by signal, continue
                        debug!("Event port interrupted by signal");
                        continue;
                    }
                    warn!("Event port get failed with errno {}: {}", err, e);
                } else {
                    warn!("Event port get failed: {}", e);
                }
                // Don't crash the thread - just log and retry
                warn!("Event port error, will retry after 1 second");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        // Skip non-file events
        if event.source != EventSource::File {
            continue;
        }

        // Use the user cookie to identify which path this event is for
        // The cookie is a hash of the path, stored when we associated the file
        let cookie = event.user as u64;

        let path = match cookie_to_path.get(&cookie) {
            Some(p) => p.clone(),
            None => {
                // Received event for a file we're no longer watching.
                // This can happen due to event ordering:
                // 1. File is deleted, we remove watcher and dissociate
                // 2. Kernel still has pending events for that file in the queue
                // 3. When we read those events, the cookie doesn't match anything
                // The FileObj pointer may be pointing to freed memory at this point,
                // so we should NOT try to dereference it. Just skip the event.
                debug!("Received event for unknown cookie: {} (have {} watchers), skipping",
                       cookie, watchers.len());
                continue;
            }
        };

        // Get file events
        let file_events = match event.file_events() {
            Some(fe) => fe,
            None => {
                debug!("No file events for {}", path);
                continue;
            }
        };

        info!("Event port received: {} events={:?}", path, file_events);

        // Now process the event (this part is safe)
        let is_dir = std::fs::metadata(&path)
            .map(|m| m.is_dir())
            .unwrap_or(false);

        // Buffer the appropriate event type
        let event_type = if file_events.contains(FileEvents::DELETE) {
            remove_watcher_recursive(&path, &mut watchers, &mut cookie_to_path);
            Some(EventPortEventType::Deleted)
        } else if file_events.contains(FileEvents::RENAME_FROM) {
            remove_watcher_recursive(&path, &mut watchers, &mut cookie_to_path);
            Some(EventPortEventType::Renamed)
        } else {
            // Look up the ORIGINAL FileObj in the HashMap
            // CRITICAL: We must refresh and re-associate the same FileObj that was originally associated
            // Moving it would invalidate the kernel's pointer

            // First check if it exists
            if !watchers.contains_key(&path) {
                warn!("Received event for unknown path: {}", path);
                None
            } else {
                // Try to refresh the FileObj
                let refresh_failed = {
                    let fo_original = watchers.get_mut(&path).unwrap();
                    fo_original.refresh().is_err()
                };

                if refresh_failed {
                    debug!("Failed to refresh {}", path);
                    remove_watcher_recursive(&path, &mut watchers, &mut cookie_to_path);
                    Some(EventPortEventType::Deleted)
                } else {
                    // Re-associate the ORIGINAL FileObj (at its stable location in the HashMap)
                    let fo_original = watchers.get(&path).unwrap();
                    let cookie = path_to_cookie(&path);
                    if let Err(e) = ep.associate_file(fo_original.as_ref(), flags, cookie as usize) {
                        warn!("Failed to re-associate {}: {}", path, e);
                    }

                    // If it's a directory and it was modified, rescan it
                    if file_events.contains(FileEvents::MODIFIED) && is_dir {
                        debug!("Rescanning directory: {}", path);
                        if let Err(e) = rescan_directory(
                            &ep,
                            &path,
                            &mut watchers,
                            &mut cookie_to_path,
                            flags,
                            &data,
                            &root,
                            &cells,
                            &ignore_specs,
                        ) {
                            warn!("Failed to rescan directory {}: {}", path, e);
                        }
                        // Don't buffer the directory modification itself
                        // rescan_directory already generated the appropriate events
                        None
                    } else {
                        // Only buffer file modifications, not directory modifications
                        if is_dir {
                            None
                        } else {
                            Some(EventPortEventType::Modified)
                        }
                    }
                }
            }
        };

        // Buffer the event
        if let Some(event_type) = event_type {
            // Acquire lock briefly to add event to buffer
            match data.lock() {
                Ok(mut guard) => {
                    if let Ok(event_data) = &mut *guard {
                        if let Err(e) = event_data.process(
                            path,
                            event_type,
                            is_dir,
                            &root,
                            &cells,
                            &ignore_specs,
                        ) {
                            warn!("Failed to process event: {:?}", e);
                            *guard = Err(e);
                        }
                    }
                    // Drop the lock immediately
                }
                Err(e) => {
                    warn!("Failed to acquire lock for event buffering (poisoned): {}", e);
                    // Lock is poisoned, stop the thread
                    return Err(anyhow::anyhow!("Event data lock poisoned"));
                }
            }
        }
    }
}

/// Recursively add watchers for a path and all its descendants
fn add_watchers_recursive(
    ep: &EventPort,
    path: &Utf8Path,
    watchers: &mut HashMap<Utf8PathBuf, Box<FileObj>>,
    cookie_to_path: &mut HashMap<u64, Utf8PathBuf>,
    flags: FileEvents,
) -> anyhow::Result<()> {
    // Create watcher for this path
    let fo = FileObj::new(path)?;

    // IMPORTANT: Box the FileObj to keep it at a stable heap location.
    // Insert into HashMap FIRST, then associate from the HashMap entry.
    // This ensures the kernel gets a pointer to the FileObj at its stable location.
    // If we associate first and then move into the HashMap, the kernel's pointer
    // becomes invalid.
    let path_owned = path.to_owned();
    watchers.insert(path_owned.clone(), Box::new(fo));
    let fo_ref = watchers.get(&path_owned).unwrap();

    // Use a hash of the path as a cookie to identify this FileObj when events arrive
    let cookie = path_to_cookie(&path_owned);
    cookie_to_path.insert(cookie, path_owned.clone());

    ep.associate_file(fo_ref.as_ref(), flags, cookie as usize)?;

    debug!("Watching: {} (cookie={})", path, cookie);

    // If it's a directory, recurse into it
    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    debug!("Failed to read directory entry: {}", e);
                    continue;
                }
            };

            // Try to convert path to UTF-8, skip if it fails
            let entry_path = match Utf8PathBuf::try_from(entry.path()) {
                Ok(p) => p,
                Err(_) => {
                    // Skip files with non-UTF-8 names
                    debug!("Skipping non-UTF-8 path: {:?}", entry.path());
                    continue;
                }
            };

            // Skip errors on individual entries (e.g., permission denied)
            if let Err(e) = add_watchers_recursive(ep, &entry_path, watchers, cookie_to_path, flags) {
                debug!("Failed to watch {}: {}", entry_path, e);
            }
        }
    }

    Ok(())
}

/// Rescan a directory and update watchers for added/removed entries
fn rescan_directory(
    ep: &EventPort,
    dir_path: &Utf8Path,
    watchers: &mut HashMap<Utf8PathBuf, Box<FileObj>>,
    cookie_to_path: &mut HashMap<u64, Utf8PathBuf>,
    flags: FileEvents,
    data: &Arc<Mutex<buck2_error::Result<EventPortFileData>>>,
    root: &ProjectRoot,
    cells: &CellResolver,
    ignore_specs: &HashMap<CellName, IgnoreSet>,
) -> anyhow::Result<()> {
    // Get current directory contents
    let mut current_entries = HashSet::new();

    for entry in std::fs::read_dir(dir_path)? {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to read directory entry in {}: {}", dir_path, e);
                continue;
            }
        };

        // Skip non-UTF-8 paths
        let entry_path = match Utf8PathBuf::try_from(entry.path()) {
            Ok(p) => p,
            Err(_) => {
                debug!("Skipping non-UTF-8 path in {}: {:?}", dir_path, entry.path());
                continue;
            }
        };

        current_entries.insert(entry_path.clone());

        // If this entry is not already watched, add it
        if !watchers.contains_key(&entry_path) {
            debug!("Adding new watcher for {}", entry_path);

            // Determine if it's a directory
            let is_dir = std::fs::metadata(&entry_path)
                .map(|m| m.is_dir())
                .unwrap_or(false);

            // Add watcher
            if let Err(e) = add_watchers_recursive(ep, &entry_path, watchers, cookie_to_path, flags) {
                debug!("Failed to add watcher for {}: {}", entry_path, e);
            } else {
                // Buffer a "created" event for this new file/directory
                if let Ok(mut guard) = data.lock() {
                    if let Ok(event_data) = &mut *guard {
                        if let Err(e) = event_data.process(
                            entry_path,
                            EventPortEventType::Created,
                            is_dir,
                            root,
                            cells,
                            ignore_specs,
                        ) {
                            warn!("Failed to process created event: {:?}", e);
                            *guard = Err(e);
                        }
                    }
                }
            }
        }
    }

    // Find and remove watchers for entries that no longer exist
    let to_remove: Vec<Utf8PathBuf> = watchers
        .keys()
        .filter(|k| {
            // Check if this path is a descendant of dir_path and no longer exists
            k.as_path().starts_with(dir_path)
                && k.as_path() != dir_path
                && !current_entries.contains(*k)
                && !k
                    .ancestors()
                    .skip(1) // Skip self
                    .take_while(|p| *p != dir_path)
                    .any(|p| current_entries.contains(p))
        })
        .cloned()
        .collect();

    for path in to_remove {
        debug!("Removing watcher for {} (no longer exists)", path);

        // Determine if it was a directory (we can't check metadata since it's deleted)
        // We'll check if any watchers have it as a prefix, which would indicate it was a directory
        let was_dir = watchers.keys().any(|k| k.as_path().starts_with(&path) && k != &path);

        // Remove from tracking structures
        // Note: We don't call dissociate_file() because file associations use one-shot semantics
        // and are automatically removed when events are delivered. Any pending events in the
        // queue will be safely ignored in the main loop when their cookies don't match.
        watchers.remove(&path);
        let cookie = path_to_cookie(&path);
        cookie_to_path.remove(&cookie);

        // Buffer a "deleted" event for this removed file/directory
        if let Ok(mut guard) = data.lock() {
            if let Ok(event_data) = &mut *guard {
                if let Err(e) = event_data.process(
                    path,
                    EventPortEventType::Deleted,
                    was_dir,
                    root,
                    cells,
                    ignore_specs,
                ) {
                    warn!("Failed to process deleted event: {:?}", e);
                    *guard = Err(e);
                }
            }
        }
    }

    Ok(())
}

/// Remove a watcher and all watchers for descendants (if it's a directory)
/// NOTE: We don't call dissociate_file() here because:
/// 1. File events use one-shot semantics - associations are auto-removed when events are delivered
/// 2. We're usually called after receiving a DELETE/RENAME event, so already dissociated
/// 3. Calling dissociate on an already-dissociated file just generates ENOENT errors
/// 4. For files that haven't had events yet, leaving them associated is harmless - the kernel
///    will drop the association when it realizes the file is gone
fn remove_watcher_recursive(
    path: &Utf8Path,
    watchers: &mut HashMap<Utf8PathBuf, Box<FileObj>>,
    cookie_to_path: &mut HashMap<u64, Utf8PathBuf>,
) {
    // Safety check: never remove everything if path is empty
    if path.as_str().is_empty() {
        eprintln!("Error: remove_watcher_recursive called with empty path, ignoring");
        return;
    }

    // Remove the path itself
    watchers.remove(path);
    let cookie = path_to_cookie(path);
    cookie_to_path.remove(&cookie);

    // Remove all descendants (use proper path comparison, not string prefix)
    let to_remove: Vec<Utf8PathBuf> = watchers
        .keys()
        .filter(|k| {
            // Use Utf8Path's starts_with which properly handles path components
            // This avoids false matches like "/tmp/foo" matching "/tmp/foobar"
            k.as_path().starts_with(path) && k.as_path() != path
        })
        .cloned()
        .collect();

    for p in to_remove {
        watchers.remove(&p);
        let cookie = path_to_cookie(&p);
        cookie_to_path.remove(&cookie);
    }
}
