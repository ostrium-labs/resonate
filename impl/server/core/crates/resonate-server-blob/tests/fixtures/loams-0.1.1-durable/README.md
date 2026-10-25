# Loams 0.1.1 durable keyspace fixture

`keys.ndjson` holds every key that Loams' embedded Resonate wrote to TiKV under
its durable root. Each key is stored relative to the root `resonate-durable/`,
and both keys and values are hex. `expect.json` holds what Loams answered to
`promise.get` for each promise.

`tests/tikv.rs` `tikv_store_reads_loams_010_fixture` loads the keys under a
fresh root and checks that the fork's store reads every promise as Loams did.
It then settles the pending one over its 0.1.1 revision and sweeps the
expired commit tokens.

## Why 0.1.1

Loams 0.1.0 and 0.1.1 have the same TiKV store: `crates/loams-durable/src/tikv.rs`
and `loams-tikv`'s token layout. Between the two tags, the only diff is
keyspace creation and the `search` switch. The 0.1.0 release image was built
without the `durable-tikv` feature, so this fixture comes from the 0.1.1
image.

## How it was taken (2026-10-24)

```sh
test/tikv/up.sh 23790 20260
podman run -d --network host --entrypoint loams ghcr.io/ostrium-labs/loams:0.1.1 dev \
  --data-dir /tmp/loamsdata --listen 127.0.0.1:18980 --flight-sql-listen 127.0.0.1:18982 \
  --durable-listen 127.0.0.1:18901 --durable-store tikv://127.0.0.1:23790/loams_durable
# The requests below went to POST http://127.0.0.1:18901/ (protocol 2026-04-01):
#   promise.create fx-pending  (headers, data, tags {"app":"fixture"})
#   promise.create + settle fx-resolved (resolved) and fx-rejected (rejected)
#   promise.create fx-timeout  (timeout 1 s; read after it passed: rejected_timedout)
#   promise.create fx-timer    (resonate:timer; read after: resolved)
#   promise.create fx-task     (resonate:target poll://any@fixture)
#   schedule.create fx-schedule (0 * * * *)
TEST_TIKV_PD=127.0.0.1:23790 TEST_TIKV_KEYSPACE=loams_durable DUMP_ROOT=resonate-durable/ \
  DUMP_OUT=keys.ndjson cargo test -p resonate-server-blob --features tikv --test tikv \
  dump_root_as_fixture -- --ignored
```
