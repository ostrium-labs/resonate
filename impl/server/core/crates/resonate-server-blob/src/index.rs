//! The tag and state index: what `promise.search` and `task.search` read on a
//! store that has one ([`StoreIndex`]), so a search costs O(results), not
//! O(store).
//!
//! # Entries
//!
//! An entry is a name with no value, in the index's own namespace (on TiKV,
//! `root ‖ "resonate/ix/" ‖ name`). Every name starts `<prefix>ix/`, where
//! `<prefix>` is the deployment's [`KeySpace`] prefix:
//!
//! ```text
//! ix/v1                             the marker: every document is indexed
//! ix/p/s/<st>/<id>                  a promise, by stored state
//! ix/p/x/<deadline:020>/<id>        a pending promise, by deadline
//! ix/p/t/<tag>/s/<st>/<id>          the same two, under each of its tags
//! ix/p/t/<tag>/x/<deadline:020>/<id>
//! ix/k/s/<st>/<id>                  a task, by state
//! ix/o/<origin>                     an origin holding an id too long for a key
//! ```
//!
//! `<st>` is the state's digit in the document codec (promises: 0 pending,
//! 1 resolved, 2 rejected, 3 rejected_canceled, 4 rejected_timedout; tasks:
//! 0 pending, 1 acquired, 2 suspended, 3 halted, 4 fulfilled). `<tag>` is
//! `key=value`, each half percent-encoded as document keys are, so neither
//! `/` nor `=` can occur inside it; a pair longer than [`MAX_TAG`] is named
//! by a hash instead (`~<fnv64>`). The id comes last and raw, so entries
//! under one range sort exactly as ids do, which is the order every search
//! pages in. An id longer than [`MAX_ID`] bytes has no entries of its own:
//! its origin gets an `o` entry, and every search reads those documents too.
//!
//! # Searching
//!
//! The index finds candidates; the document decides. A search reads the
//! ranges that can hold a match, merges them in id order after the cursor,
//! reads the documents the candidates live in, and keeps a candidate only if
//! its record, projected at `now`, passes the whole filter, exactly as the
//! scan does. So an entry can make a search read a document it did not
//! need, but never return a record it should not.
//!
//! The ranges for a state filter follow the projection: a promise that is
//! pending but past its deadline *is* timed out (or resolved, for a timer),
//! so `rejected_timedout` and `resolved` also read the pending promises
//! whose deadline has passed (the `x` range up to `now`), and `pending`
//! skips them. A tag filter reads the ranges under its first tag (smallest
//! key) and checks the others against the document. All of this happens in
//! one [`IndexView`], so the entries and the documents agree.
//!
//! Cost: one range read per state range (one, or five with no state filter),
//! one more for the expired pending promises when the projection needs
//! them, and one batch read of the documents behind the page. Expired
//! pending promises that no sweep has written down yet, and promises under
//! the first tag that fail the others, are read and dropped: that is the
//! O(results) claim's fine print, and it is the same work SQL's
//! `effective_state_sql` does.
//!
//! # Writing
//!
//! The applier writes a document and the change in its entries
//! ([`delta`]) in one [`StoreIndex::put_indexed`]: either both land or
//! neither does. Entries of a promise or task are only ever written by a
//! commit of its own origin's document, which the document's compare-and-set
//! serializes, so writers on other origins never touch the same entry.
//!
//! # Existing data
//!
//! A store written before the index (a Loams 0.1.x keyspace) has documents
//! and no entries. Until the marker exists, searches read every document as
//! before, and the first one starts [`rebuild`]: every document's entries are
//! written with [`StoreIndex::index_if_match`] against the revision they were
//! computed from, then the marker. Writers keep the entries of the documents
//! they change exact throughout: a document written by an index-aware writer
//! holds at most the entries its current version implies (a delta from an
//! unindexed version cannot add what was never there), and the rebuild adds
//! the rest. Two rebuilds at once, from two instances, write the same
//! entries.
//!
//! A server without the index (Loams 0.1.x, or this one on an object store)
//! does not maintain entries. Writing an indexed store with one leaves the
//! index behind: delete the marker and the next search rebuilds it.
//!
//! # Dependencies
//!
//! The store port, the codec, the kernel's document types and [`KeySpace`].
//!
//! # Dependants
//!
//! The applier ([`delta`]), the scan service (the searches and the rebuild)
//! and `Server` (`debug.reset`).

use std::collections::{BTreeSet, HashMap, VecDeque};

use resonate_core::types::{
    PromiseRecord, PromiseSearchData, PromiseState, TaskRecord, TaskSearchData, TaskState,
};

use crate::applier::{encode_key, KeySpace};
use crate::codec;
use crate::kernel::state::{OriginDoc, PromiseDoc, TaskDoc};
use crate::store::{IndexDelta, IndexView, Store, StoreError, StoreIndex};

/// The longest id that is put in an entry. Longer ones are found through
/// their origin's `o` entry. TiKV refuses keys over 8 KiB by default; this
/// leaves room for the root, the prefix and a tag.
pub const MAX_ID: usize = 1024;
/// The longest encoded `key=value` a tag entry carries before it is hashed.
pub const MAX_TAG: usize = 256;
/// Bumped when the entry layout changes: a store marked with an older
/// version is rebuilt.
pub const INDEX_VERSION: u32 = 1;

const DEADLINE_WIDTH: usize = 20;
const PROMISE_STATES: [PromiseState; 5] = [
    PromiseState::Pending,
    PromiseState::Resolved,
    PromiseState::Rejected,
    PromiseState::RejectedCanceled,
    PromiseState::RejectedTimedout,
];
const TASK_STATES: [TaskState; 5] = [
    TaskState::Pending,
    TaskState::Acquired,
    TaskState::Suspended,
    TaskState::Halted,
    TaskState::Fulfilled,
];

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------

/// Everything the index holds lives under this.
pub fn root(keys: &KeySpace) -> String {
    format!("{}ix/", keys.prefix)
}

/// The marker: present once every document has its entries.
pub fn marker(keys: &KeySpace) -> String {
    format!("{}ix/v{INDEX_VERSION}", keys.prefix)
}

/// The origin of an id: everything before the first `':'`.
pub fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

fn promise_digit(s: PromiseState) -> char {
    match s {
        PromiseState::Pending => '0',
        PromiseState::Resolved => '1',
        PromiseState::Rejected => '2',
        PromiseState::RejectedCanceled => '3',
        PromiseState::RejectedTimedout => '4',
    }
}

fn task_digit(s: TaskState) -> char {
    match s {
        TaskState::Pending => '0',
        TaskState::Acquired => '1',
        TaskState::Suspended => '2',
        TaskState::Halted => '3',
        TaskState::Fulfilled => '4',
    }
}

/// A tag as it appears in an entry name.
fn tag_part(key: &str, value: &str) -> String {
    let part = format!("{}={}", encode_key(key), encode_key(value));
    if part.len() <= MAX_TAG {
        return part;
    }
    // A candidate is checked against its document, so a collision costs a
    // read, never a wrong answer.
    let mut joined = String::with_capacity(key.len() + value.len() + 1);
    joined.push_str(key);
    joined.push('\0');
    joined.push_str(value);
    format!("~{}", codec::origin_hash(&joined))
}

/// The promise ranges: `<prefix>ix/p/` or, under a tag, `<prefix>ix/p/t/<tag>/`.
fn promise_scope(keys: &KeySpace, tag: Option<(&str, &str)>) -> String {
    match tag {
        None => format!("{}p/", root(keys)),
        Some((k, v)) => format!("{}p/t/{}/", root(keys), tag_part(k, v)),
    }
}

fn deadline(at: i64) -> String {
    format!("{:0width$}", at.max(0), width = DEADLINE_WIDTH)
}

fn overflow_prefix(keys: &KeySpace) -> String {
    format!("{}o/", root(keys))
}

fn overflow_entry(keys: &KeySpace, origin: &str) -> String {
    format!("{}{}", overflow_prefix(keys), encode_key(origin))
}

/// The smallest string greater than every string that starts with `prefix`.
/// Entry names are ASCII up to the id, so bumping the last byte suffices.
fn successor(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    match bytes.last_mut() {
        Some(last) if *last < 0x7f => *last += 1,
        _ => return String::new(),
    }
    String::from_utf8(bytes).unwrap_or_default()
}

/// The smallest string greater than `s`.
fn after(s: &str) -> String {
    format!("{s}\0")
}

// ---------------------------------------------------------------------------
// The entries of a document
// ---------------------------------------------------------------------------

fn promise_entries(keys: &KeySpace, id: &str, p: &PromiseDoc, out: &mut BTreeSet<String>) {
    if id.len() > MAX_ID {
        return;
    }
    let st = promise_digit(p.state);
    let scopes = std::iter::once(promise_scope(keys, None)).chain(
        p.tags
            .iter()
            .map(|(k, v)| promise_scope(keys, Some((k.as_str(), v.as_str())))),
    );
    for scope in scopes {
        out.insert(format!("{scope}s/{st}/{id}"));
        if p.state == PromiseState::Pending {
            out.insert(format!("{scope}x/{}/{id}", deadline(p.timeout_at)));
        }
    }
}

fn task_entries(keys: &KeySpace, id: &str, t: &TaskDoc, out: &mut BTreeSet<String>) {
    if id.len() > MAX_ID {
        return;
    }
    out.insert(format!("{}k/s/{}/{id}", root(keys), task_digit(t.state)));
}

fn has_overflow(doc: &OriginDoc) -> bool {
    doc.promises.keys().chain(doc.tasks.keys()).any(|id| id.len() > MAX_ID)
}

/// Every entry `doc` implies.
pub fn entries(keys: &KeySpace, origin: &str, doc: &OriginDoc) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (id, p) in &doc.promises {
        promise_entries(keys, id, p, &mut out);
    }
    for (id, t) in &doc.tasks {
        task_entries(keys, id, t, &mut out);
    }
    if has_overflow(doc) {
        out.insert(overflow_entry(keys, origin));
    }
    out
}

/// What a write from `before` to `after` changes in the index. Only the
/// promises and tasks that differ are looked at, so a write costs what it
/// changes, not the size of its document.
pub fn delta(keys: &KeySpace, origin: &str, before: &OriginDoc, after: &OriginDoc) -> IndexDelta {
    let mut old = BTreeSet::new();
    let mut new = BTreeSet::new();
    for (id, p) in &before.promises {
        if after.promises.get(id) != Some(p) {
            promise_entries(keys, id, p, &mut old);
        }
    }
    for (id, p) in &after.promises {
        if before.promises.get(id) != Some(p) {
            promise_entries(keys, id, p, &mut new);
        }
    }
    for (id, t) in &before.tasks {
        if after.tasks.get(id) != Some(t) {
            task_entries(keys, id, t, &mut old);
        }
    }
    for (id, t) in &after.tasks {
        if before.tasks.get(id) != Some(t) {
            task_entries(keys, id, t, &mut new);
        }
    }
    match (has_overflow(before), has_overflow(after)) {
        (false, true) => {
            new.insert(overflow_entry(keys, origin));
        }
        (true, false) => {
            old.insert(overflow_entry(keys, origin));
        }
        _ => {}
    }
    IndexDelta {
        put: new.difference(&old).cloned().collect(),
        delete: old.difference(&new).cloned().collect(),
    }
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// One stream of candidate ids, ascending.
struct Source {
    /// The next read starts here (inclusive).
    start: String,
    end: String,
    /// Bytes of an entry before its id.
    strip: usize,
    buf: VecDeque<String>,
    done: bool,
}

impl Source {
    /// The range `[base, successor(base))`, from just after `cursor`.
    fn range(base: String, cursor: Option<&str>) -> Self {
        let end = successor(&base);
        let start = match cursor {
            Some(c) => after(&format!("{base}{c}")),
            None => base.clone(),
        };
        Self {
            start,
            end,
            strip: base.len(),
            buf: VecDeque::new(),
            done: false,
        }
    }

    /// Ids that are already known, sorted.
    fn fixed(mut ids: Vec<String>) -> Self {
        ids.sort();
        ids.dedup();
        Self {
            start: String::new(),
            end: String::new(),
            strip: 0,
            buf: ids.into(),
            done: true,
        }
    }

    async fn fill(&mut self, view: &mut dyn IndexView, page: usize) -> Result<(), StoreError> {
        let names = view.scan(&self.start, &self.end, page).await?;
        if names.len() < page {
            self.done = true;
        }
        if let Some(last) = names.last() {
            self.start = after(last);
        }
        for name in names {
            if let Some(id) = name.get(self.strip..) {
                self.buf.push_back(id.to_string());
            }
        }
        Ok(())
    }
}

/// Every entry in `[start, end)`, page by page.
async fn scan_all(
    view: &mut dyn IndexView,
    start: String,
    end: &str,
) -> Result<Vec<String>, StoreError> {
    const PAGE: usize = 1_000;
    let mut out = Vec::new();
    let mut start = start;
    loop {
        let page = view.scan(&start, end, PAGE).await?;
        let full = page.len() == PAGE;
        if let Some(last) = page.last() {
            start = after(last);
        }
        out.extend(page);
        if !full {
            return Ok(out);
        }
    }
}

/// The documents read by one search, by origin.
struct Docs<'k> {
    keys: &'k KeySpace,
    loaded: HashMap<String, Option<OriginDoc>>,
}

impl<'k> Docs<'k> {
    fn new(keys: &'k KeySpace) -> Self {
        Self {
            keys,
            loaded: HashMap::new(),
        }
    }

    async fn load<'a>(
        &mut self,
        view: &mut dyn IndexView,
        origins: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StoreError> {
        let mut missing: Vec<String> = Vec::new();
        for origin in origins {
            if !self.loaded.contains_key(origin) && !missing.iter().any(|m| m == origin) {
                missing.push(origin.to_string());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let keys: Vec<String> = missing.iter().map(|o| self.keys.doc_key(o)).collect();
        let found = view.get_many(&keys).await?;
        for ((origin, key), got) in missing.into_iter().zip(keys).zip(found) {
            let doc = match got {
                Some((bytes, _)) => Some(codec::decode(&bytes, &origin).map_err(|e| {
                    StoreError::Unavailable(format!("document {key} unreadable: {e}"))
                })?),
                None => None,
            };
            self.loaded.insert(origin, doc);
        }
        Ok(())
    }

    fn get(&self, origin: &str) -> Option<&OriginDoc> {
        self.loaded.get(origin).and_then(Option::as_ref)
    }
}

/// The ids in the overflow documents that `pick` selects, after `cursor`.
async fn overflow_ids(
    view: &mut dyn IndexView,
    docs: &mut Docs<'_>,
    cursor: Option<&str>,
    pick: impl Fn(&OriginDoc) -> Vec<String>,
) -> Result<Vec<String>, StoreError> {
    let prefix = overflow_prefix(docs.keys);
    let names = scan_all(view, prefix.clone(), &successor(&prefix)).await?;
    let origins: Vec<String> = names
        .iter()
        .filter_map(|n| n.strip_prefix(&prefix))
        .filter_map(crate::applier::decode_key)
        .collect();
    docs.load(view, origins.iter().map(String::as_str)).await?;
    let mut ids = Vec::new();
    for origin in &origins {
        if let Some(doc) = docs.get(origin) {
            ids.extend(
                pick(doc)
                    .into_iter()
                    .filter(|id| id.len() > MAX_ID && cursor.is_none_or(|c| id.as_str() > c)),
            );
        }
    }
    Ok(ids)
}

/// Merge `sources` in id order and keep the first `want` candidates `keep`
/// accepts, reading their documents in batches.
async fn collect<R>(
    view: &mut dyn IndexView,
    docs: &mut Docs<'_>,
    mut sources: Vec<Source>,
    want: usize,
    keep: impl Fn(&OriginDoc, &str) -> Option<R>,
) -> Result<Vec<R>, StoreError> {
    let page = want.clamp(1, 1_000);
    let mut out = Vec::new();
    while out.len() < want {
        let need = want - out.len();
        let mut chunk: Vec<String> = Vec::with_capacity(need);
        while chunk.len() < need {
            for s in sources.iter_mut() {
                if s.buf.is_empty() && !s.done {
                    s.fill(view, page).await?;
                }
            }
            let Some(min) = sources.iter().filter_map(|s| s.buf.front()).min().cloned() else {
                break;
            };
            for s in sources.iter_mut() {
                if s.buf.front() == Some(&min) {
                    s.buf.pop_front();
                }
            }
            chunk.push(min);
        }
        if chunk.is_empty() {
            break;
        }
        docs.load(view, chunk.iter().map(|id| origin_of(id))).await?;
        for id in &chunk {
            if let Some(r) = docs.get(origin_of(id)).and_then(|doc| keep(doc, id)) {
                out.push(r);
            }
        }
    }
    Ok(out)
}

/// One page of `promise.search` through the index: up to `limit + 1`
/// matches after the cursor, in id order, projected at `now`. The caller
/// pages it, exactly as it pages the scan.
pub async fn search_promises(
    view: &mut dyn IndexView,
    keys: &KeySpace,
    q: &PromiseSearchData,
    limit: usize,
    now: i64,
) -> Result<Vec<PromiseRecord>, StoreError> {
    let cursor = q.cursor.as_deref();
    // The first tag by key picks the ranges; the rest are checked against
    // the document.
    let tag = q
        .tags
        .as_ref()
        .and_then(|t| t.iter().min_by(|a, b| a.0.cmp(b.0)))
        .map(|(k, v)| (k.as_str(), v.as_str()));
    let scope = promise_scope(keys, tag);
    let range = |st: PromiseState| {
        Source::range(format!("{scope}s/{}/", promise_digit(st)), cursor)
    };
    let mut sources: Vec<Source> = match q.state {
        None => PROMISE_STATES.iter().map(|s| range(*s)).collect(),
        Some(st) => vec![range(st)],
    };
    // Pending promises past their deadline read as resolved (a timer) or
    // timed out (anything else) until a sweep writes that down.
    if matches!(
        q.state,
        Some(PromiseState::Resolved | PromiseState::RejectedTimedout)
    ) {
        let base = format!("{scope}x/");
        let strip = base.len() + DEADLINE_WIDTH + 1;
        let end = format!("{base}{}", deadline(now.saturating_add(1)));
        let ids: Vec<String> = scan_all(view, base, &end)
            .await?
            .into_iter()
            .filter_map(|n| n.get(strip..).map(str::to_string))
            .filter(|id| cursor.is_none_or(|c| id.as_str() > c))
            .collect();
        sources.push(Source::fixed(ids));
    }
    let mut docs = Docs::new(keys);
    let long = overflow_ids(view, &mut docs, cursor, |d| d.promises.keys().cloned().collect())
        .await?;
    if !long.is_empty() {
        sources.push(Source::fixed(long));
    }
    collect(view, &mut docs, sources, limit + 1, |doc, id| {
        let mut record = doc.promises.get(id)?.to_record(id);
        record.project(now);
        if q.state.is_some_and(|s| record.state != s) {
            return None;
        }
        if let Some(want) = &q.tags {
            if !want
                .iter()
                .all(|(k, v)| record.tags.get(k).is_some_and(|got| got == v))
            {
                return None;
            }
        }
        Some(record)
    })
    .await
}

/// One page of `task.search` through the index: up to `limit + 1` matches.
pub async fn search_tasks(
    view: &mut dyn IndexView,
    keys: &KeySpace,
    q: &TaskSearchData,
    limit: usize,
) -> Result<Vec<TaskRecord>, StoreError> {
    let cursor = q.cursor.as_deref();
    let range = |st: TaskState| {
        Source::range(format!("{}k/s/{}/", root(keys), task_digit(st)), cursor)
    };
    let mut sources: Vec<Source> = match q.state {
        None => TASK_STATES.iter().map(|s| range(*s)).collect(),
        Some(st) => vec![range(st)],
    };
    let mut docs = Docs::new(keys);
    let long = overflow_ids(view, &mut docs, cursor, |d| d.tasks.keys().cloned().collect()).await?;
    if !long.is_empty() {
        sources.push(Source::fixed(long));
    }
    collect(view, &mut docs, sources, limit + 1, |doc, id| {
        let task = doc.tasks.get(id)?;
        if q.state.is_some_and(|s| task.state != s) {
            return None;
        }
        Some(task.to_record(id))
    })
    .await
}

// ---------------------------------------------------------------------------
// Marker, rebuild, reset
// ---------------------------------------------------------------------------

/// Whether the marker is there: every document has its entries.
pub async fn is_built(ix: &dyn StoreIndex, keys: &KeySpace) -> Result<bool, StoreError> {
    let marker = marker(keys);
    let mut view = ix.view().await?;
    Ok(!view.scan(&marker, &after(&marker), 1).await?.is_empty())
}

/// How many times the rebuild retries one document a writer keeps changing.
const REBUILD_ATTEMPTS: u32 = 32;

/// Writes every document's entries, each against the revision it was read
/// at, then the marker. Idempotent, and safe beside writers and beside
/// another rebuild. Returns how many documents it indexed.
pub async fn rebuild(
    store: &dyn Store,
    ix: &dyn StoreIndex,
    keys: &KeySpace,
) -> Result<u64, StoreError> {
    let doc_keys = store.list(&keys.doc_prefix(), usize::MAX).await?;
    let mut indexed = 0;
    for key in doc_keys {
        let Some(origin) = keys.origin_of_doc_key(&key) else {
            continue;
        };
        let mut attempt = 0;
        loop {
            let Some((bytes, etag)) = store.get(&key).await? else {
                break;
            };
            let doc = codec::decode(&bytes, &origin).map_err(|e| {
                StoreError::Unavailable(format!("document {key} unreadable: {e}"))
            })?;
            let delta = IndexDelta {
                put: entries(keys, &origin, &doc).into_iter().collect(),
                delete: Vec::new(),
            };
            if delta.is_empty() {
                break;
            }
            match ix.index_if_match(&key, &etag, &delta).await {
                Ok(()) => {
                    indexed += 1;
                    break;
                }
                // A writer got there first, and its write kept the entries
                // of the version it wrote: read that one and go again.
                Err(StoreError::PreconditionFailed | StoreError::Conflict)
                    if attempt < REBUILD_ATTEMPTS =>
                {
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
    ix.index_apply(&IndexDelta {
        put: vec![marker(keys)],
        delete: Vec::new(),
    })
    .await?;
    Ok(indexed)
}

/// Deletes every entry and marks the index built: what `debug.reset` does
/// once the documents are gone.
pub async fn reset(ix: &dyn StoreIndex, keys: &KeySpace) -> Result<(), StoreError> {
    let prefix = root(keys);
    let end = successor(&prefix);
    loop {
        let mut view = ix.view().await?;
        let names = view.scan(&prefix, &end, 1_000).await?;
        if names.is_empty() {
            break;
        }
        ix.index_apply(&IndexDelta {
            put: Vec::new(),
            delete: names,
        })
        .await?;
    }
    ix.index_apply(&IndexDelta {
        put: vec![marker(keys)],
        delete: Vec::new(),
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::PromiseValue;
    use std::collections::BTreeMap;

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    fn promise(state: PromiseState, timeout_at: i64, tags: &[(&str, &str)]) -> PromiseDoc {
        PromiseDoc {
            state,
            param: PromiseValue::default(),
            value: PromiseValue::default(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            timeout_at,
            created_at: 0,
            settled_at: None,
            callbacks: Vec::new(),
            listeners: Vec::new(),
        }
    }

    fn doc(promises: Vec<(&str, PromiseDoc)>) -> OriginDoc {
        OriginDoc {
            promises: promises
                .into_iter()
                .map(|(id, p)| (id.to_string(), p))
                .collect::<BTreeMap<_, _>>(),
            ..Default::default()
        }
    }

    #[test]
    fn a_pending_promise_is_entered_by_state_deadline_and_tag() {
        let d = doc(vec![(
            "o:a",
            promise(PromiseState::Pending, 500, &[("loams:run", "r/1")]),
        )]);
        let got: Vec<String> = entries(&keys(), "o", &d).into_iter().collect();
        assert_eq!(
            got,
            vec![
                "p/ix/p/s/0/o:a".to_string(),
                "p/ix/p/t/loams%3Arun=r%2F1/s/0/o:a".to_string(),
                "p/ix/p/t/loams%3Arun=r%2F1/x/00000000000000000500/o:a".to_string(),
                "p/ix/p/x/00000000000000000500/o:a".to_string(),
            ]
        );
    }

    #[test]
    fn settling_moves_the_state_entries_and_drops_the_deadline() {
        let before = doc(vec![("o:a", promise(PromiseState::Pending, 500, &[]))]);
        let after = doc(vec![("o:a", promise(PromiseState::Resolved, 500, &[]))]);
        let d = delta(&keys(), "o", &before, &after);
        assert_eq!(d.put, vec!["p/ix/p/s/1/o:a".to_string()]);
        assert_eq!(
            d.delete,
            vec![
                "p/ix/p/s/0/o:a".to_string(),
                "p/ix/p/x/00000000000000000500/o:a".to_string()
            ]
        );
        assert!(delta(&keys(), "o", &after, &after).is_empty());
    }

    #[test]
    fn a_delta_replayed_over_the_entries_of_before_gives_the_entries_of_after() {
        let before = doc(vec![
            ("o:a", promise(PromiseState::Pending, 500, &[("t", "1")])),
            ("o:b", promise(PromiseState::Pending, 700, &[])),
        ]);
        let after = doc(vec![
            ("o:a", promise(PromiseState::Rejected, 500, &[("t", "1")])),
            ("o:b", promise(PromiseState::Pending, 700, &[])),
            ("o:c", promise(PromiseState::Pending, 900, &[("u", "2")])),
        ]);
        let mut set = entries(&keys(), "o", &before);
        let d = delta(&keys(), "o", &before, &after);
        for e in &d.delete {
            assert!(set.remove(e), "deletes only what is there: {e}");
        }
        set.extend(d.put);
        assert_eq!(set, entries(&keys(), "o", &after));
    }

    #[test]
    fn a_long_tag_is_hashed_and_a_long_id_goes_through_its_origin() {
        let long_value = "v".repeat(MAX_TAG);
        let long_id = format!("o:{}", "x".repeat(MAX_ID));
        let d = doc(vec![
            ("o:a", promise(PromiseState::Resolved, 0, &[("k", &long_value)])),
            (&long_id, promise(PromiseState::Resolved, 0, &[])),
        ]);
        let got = entries(&keys(), "o", &d);
        assert!(got.iter().any(|e| e.starts_with("p/ix/p/t/~")));
        assert!(got.contains("p/ix/o/o"));
        assert!(got.iter().all(|e| !e.contains(&long_id)));
        let without = doc(vec![(
            "o:a",
            promise(PromiseState::Resolved, 0, &[("k", &long_value)]),
        )]);
        assert_eq!(
            delta(&keys(), "o", &d, &without).delete,
            vec!["p/ix/o/o".to_string()]
        );
    }

    #[test]
    fn a_tag_entry_cannot_be_confused_with_another() {
        // `/` and `=` are escaped, so a key or value holding them cannot
        // split into a different pair or a different range.
        assert_ne!(tag_part("a=b", "c"), tag_part("a", "b=c"));
        assert!(!tag_part("a/s/0", "x").contains('/'));
    }

    #[test]
    fn ranges_end_where_their_prefix_does() {
        assert_eq!(successor("p/ix/p/s/0/"), "p/ix/p/s/00");
        let r = Source::range("p/ix/p/s/0/".into(), Some("o:b"));
        assert_eq!(r.start, "p/ix/p/s/0/o:b\0");
        assert!("p/ix/p/s/0/o:c" >= r.start.as_str() && "p/ix/p/s/0/o:c" < r.end.as_str());
        assert!("p/ix/p/s/0/o:b" < r.start.as_str());
    }
}
