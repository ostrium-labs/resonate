//! The [`Store`] contract, run against every store this build has: the
//! in-memory object store always, and TiKV when `TEST_TIKV_PD` names a PD
//! (feature `tikv`; the keyspace is `TEST_TIKV_KEYSPACE`, default
//! `resonate_test`, created if missing; every test works under a fresh root).
//!
//! ```text
//! TEST_TIKV_PD=127.0.0.1:2379 \
//!   cargo test -p resonate-server-blob --features tikv --test store_contract
//! ```

use std::sync::Arc;

use resonate_server_blob::store::{ObjectStoreAdapter, Store, StoreError};

/// Every store under test, each with a fresh prefix of its own.
async fn stores() -> Vec<(String, Arc<dyn Store>, String)> {
    let mut out: Vec<(String, Arc<dyn Store>, String)> = vec![(
        "memory".into(),
        Arc::new(ObjectStoreAdapter::in_memory()),
        "contract/".into(),
    )];
    #[cfg(feature = "tikv")]
    if let Some(store) = tikv().await {
        out.push(("tikv".into(), store, "contract/".into()));
    }
    out
}

#[cfg(feature = "tikv")]
async fn tikv() -> Option<Arc<dyn Store>> {
    use resonate_server_blob::tikv::{
        ensure_keyspace, keyspace::pd_http_url, TikvStore, TikvStoreCfg,
    };
    let pd = std::env::var("TEST_TIKV_PD")
        .ok()
        .filter(|s| !s.is_empty())?;
    let keyspace = std::env::var("TEST_TIKV_KEYSPACE").unwrap_or_else(|_| "resonate_test".into());
    let endpoints: Vec<String> = pd.split(',').map(str::to_owned).collect();
    ensure_keyspace(&pd_http_url(&endpoints[0]), &keyspace)
        .await
        .expect("TEST_TIKV_PD keyspace");
    let mut cfg = TikvStoreCfg::new(endpoints, keyspace);
    cfg.root = format!("contract-{:016x}/", fastrand::u64(..)).into_bytes();
    Some(Arc::new(
        TikvStore::connect(cfg).await.expect("TEST_TIKV_PD"),
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_writes_behave_as_compare_and_swap() {
    for (name, s, p) in stores().await {
        let k = format!("{p}doc");
        assert_eq!(s.get(&k).await.unwrap(), None, "{name}");
        let e1 = s.put_if_none_match(&k, b"one".to_vec()).await.unwrap();
        let (body, read) = s.get(&k).await.unwrap().unwrap();
        assert_eq!(body, b"one", "{name}");
        assert_eq!(
            read, e1,
            "{name}: the etag a read reports is the one put returned"
        );
        assert_eq!(
            s.put_if_none_match(&k, b"two".to_vec()).await,
            Err(StoreError::PreconditionFailed),
            "{name}"
        );
        let e2 = s.put_if_match(&k, b"two".to_vec(), &e1).await.unwrap();
        assert_ne!(e1, e2, "{name}");
        assert_eq!(
            s.put_if_match(&k, b"stale".to_vec(), &e1).await,
            Err(StoreError::PreconditionFailed),
            "{name}"
        );
        let e3 = s.put(&k, b"blind".to_vec()).await.unwrap();
        assert_eq!(
            s.get(&k).await.unwrap(),
            Some((b"blind".to_vec(), e3)),
            "{name}"
        );
        s.delete(&k).await.unwrap();
        s.delete(&k).await.unwrap(); // deleting what is not there succeeds
        assert_eq!(s.get(&k).await.unwrap(), None, "{name}");
        // A re-create gets a fresh version: the old etag cannot win.
        let e4 = s.put_if_none_match(&k, b"three".to_vec()).await.unwrap();
        assert_ne!(e4, e1, "{name}");
        assert_eq!(
            s.put_if_match(&k, b"x".to_vec(), &e1).await,
            Err(StoreError::PreconditionFailed),
            "{name}"
        );
        assert_eq!(
            s.put_if_match(&format!("{p}missing"), b"x".to_vec(), &e4)
                .await,
            Err(StoreError::PreconditionFailed),
            "{name}: a conditional write to a missing object is refused"
        );
        s.delete_prefix(&p).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lists_are_ordered_bounded_scoped_and_read_after_write() {
    for (name, s, p) in stores().await {
        for d in [30u32, 10, 20, 50, 40] {
            s.put(&format!("{p}t/{d:010}"), vec![]).await.unwrap();
        }
        s.put(&format!("{p}u/0000000001"), vec![]).await.unwrap();
        s.put(&format!("{p}t"), vec![]).await.unwrap();
        let got = s.list(&format!("{p}t/"), 3).await.unwrap();
        assert_eq!(
            got,
            vec![
                format!("{p}t/0000000010"),
                format!("{p}t/0000000020"),
                format!("{p}t/0000000030")
            ],
            "{name}"
        );
        assert_eq!(
            s.list(&format!("{p}t/"), 100).await.unwrap().len(),
            5,
            "{name}"
        );
        assert_eq!(
            s.list(&format!("{p}t/"), 0).await.unwrap(),
            Vec::<String>::new(),
            "{name}"
        );
        s.delete_prefix(&p).await.unwrap();
        assert!(s.list(&p, 10).await.unwrap().is_empty(), "{name}");
    }
}

/// N writers each add 1 to a counter by read–CAS, retrying on
/// `PreconditionFailed` (re-read) and `Conflict` (same write). No update may
/// be lost: the counter must end at exactly N × rounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_cas_loses_no_update() {
    for (name, s, p) in stores().await {
        let k = format!("{p}counter");
        s.put_if_none_match(&k, b"0".to_vec()).await.unwrap();
        const WRITERS: usize = 8;
        const ROUNDS: usize = 25;
        let mut handles = Vec::new();
        for _ in 0..WRITERS {
            let s = Arc::clone(&s);
            let k = k.clone();
            handles.push(tokio::spawn(async move {
                let mut lost_races = 0u32;
                for _ in 0..ROUNDS {
                    loop {
                        let (body, etag) = s.get(&k).await.unwrap().unwrap();
                        let n: u64 = std::str::from_utf8(&body).unwrap().parse().unwrap();
                        let next = (n + 1).to_string().into_bytes();
                        let mut attempt = s.put_if_match(&k, next.clone(), &etag).await;
                        while attempt == Err(StoreError::Conflict) {
                            attempt = s.put_if_match(&k, next.clone(), &etag).await;
                        }
                        match attempt {
                            Ok(_) => break,
                            Err(StoreError::PreconditionFailed) => lost_races += 1,
                            Err(e) => panic!("unexpected {e}"),
                        }
                    }
                }
                lost_races
            }));
        }
        let mut races = 0;
        for h in handles {
            races += h.await.unwrap();
        }
        let (body, _) = s.get(&k).await.unwrap().unwrap();
        let n: u64 = std::str::from_utf8(&body).unwrap().parse().unwrap();
        eprintln!("[contract] {name}: counter={n} lost_races={races}");
        assert_eq!(
            n,
            (WRITERS * ROUNDS) as u64,
            "{name}: a CAS update was lost"
        );
        s.delete_prefix(&p).await.unwrap();
    }
}
