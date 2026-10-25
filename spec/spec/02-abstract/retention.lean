import «02-abstract».«system»

/-!  # Retention — settled promises leave the store (FW5)

The machine in `system.lean` never deletes an object. That is a fine
protocol and an unbounded store: every promise a server ever created is
still a row. Retention is the server setting that lets a settled,
quiet promise LEAVE, and this file is what it means.

## A conservative extension, not an edit

Nothing in `state.lean`, `external.lean`, `internal.lean` or
`system.lean` changes, and no existing inductive gains a constructor —
adding one to `Response` or `Step` would break every exhaustive match
and every proof over them. The retained machine is a NEW layer:

  * `RState` is the old `ServerState` plus a list of tombstones;
  * `RStep` is an old `Step`, or one of two new internal steps, `prune`
    and `expire`;
  * `RResponse` is an old `Response`, or `expired id`;
  * `rstepOf` runs an old step through `Abstraction.stepOf` untouched,
    and adds exactly two things around it: a guard IN FRONT (a request
    naming a tombstoned id is answered `expired` and writes nothing)
    and a filter BEHIND (no object survives at a tombstoned id).

With no tombstones the guard never fires and the filter keeps
everything, so a run that never prunes is a run of the old machine,
response for response and state for state. That is
`rstep_base_conservative` in `04-theorems/retention.lean`, and the
corpus there checks it on the whole battery.

## What pruning removes, and what it leaves

An object is PRUNABLE when nothing can still ask about it: its promise
(projected at `now`) is settled, a retention rule selects its tags and
its age has passed, no callback or listener is still registered on it,
its task (if any) is fulfilled, and no task holds it in `resumes`.
Pruning deletes the object, strikes its id from every other promise's
`callbacks`, and leaves a TOMBSTONE.

The tombstone is the whole safety story. Without it a pruned id is
indistinguishable from one that never existed, and `promise.create`
at that id would succeed — a second promise with the first one's name,
which a client that saw the first settle would read as the same
promise going back to pending. With it, any request naming the id is
answered `expired` (the implementation's 410) until the tombstone is
swept. `no_resurrection` below is that claim, and it is the property
design 74 asks for: no pruned id is re-created while its tombstone
lives.

## When a tombstone dies

`expiresAt = prunedAt + horizon` is when the `expire` step becomes
ENABLED for it, not when it stops counting. An implementation sweeps
its tombstone table at intervals and checks it by id inside the
request's transaction, so between `expiresAt` and the sweep a request
still answers `expired`. `buried` is therefore PRESENCE in the list,
not liveness at `now`, and a tombstone lives AT LEAST until its expiry
(`tombstone_kept`) — possibly longer.

## Tag selection, and the string prefix

`Rule.selects` compares tag values by prefix. It is written over
`String.toList` and `List.isPrefixOf` rather than `String.isPrefixOf`,
which is the same function on every pair of strings but is not
reducible by the kernel, and the corpus decides over it. Same house
style as `Ident.parse` in `valid/lean/validator.lean`. -/

namespace Retention

open ServerModel (Ident Tags)
open AbstractModel (Object ServerState)
open Abstraction (Step stepOf)
open Equivalence (Request Response)

/-! ## The setting -/

/-- One retention rule. `tag = none` selects every promise; `tag = some k`
    selects a promise carrying tag `k` whose value starts with one of
    `prefixes` — or carrying `k` at all, when `prefixes` is empty.
    `age` is how long after settlement a selected promise is kept, in
    milliseconds. -/
structure Rule where
  tag      : Option String := none
  prefixes : List String   := []
  age      : Nat
  deriving Repr

/-- The server setting. NOT state, for the same reason `ServerConfig` is
    not: no step writes it and no response projects it. `horizon` is how
    long a tombstone is guaranteed to outlive its object. -/
structure Policy where
  rules   : List Rule := []
  horizon : Nat       := 0
  deriving Repr

/-- Retention off: no rule selects anything, so nothing is ever
    prunable. -/
def Policy.off : Policy := {}

/-- `String.isPrefixOf`, in a form the kernel reduces. See the header. -/
def isPrefix (p v : String) : Bool :=
  p.toList.isPrefixOf v.toList

def Rule.selects (r : Rule) (tags : Tags) : Bool :=
  match r.tag with
  | none   => true
  | some k =>
      match tags.get? k with
      | none   => false
      | some v => r.prefixes.isEmpty || r.prefixes.any (isPrefix · v)

/-- The running minimum the fold keeps. -/
def minAge : Option Nat → Nat → Option Nat
  | none,   a => some a
  | some b, a => some (min b a)

/-- The age that applies to a promise with these tags: the SMALLEST over
    every rule that selects them, `none` when none does. Smallest, so
    that adding a rule can only make retention shorter for what it
    selects — an operator who writes a short rule for one tag is not
    overridden by a long catch-all. A fold rather than `List.min?`, so
    the statement does not depend on that library function's shape. -/
def Policy.age? (π : Policy) (tags : Tags) : Option Nat :=
  π.rules.foldl (fun acc r => if r.selects tags then minAge acc r.age else acc) none

/-! ## The retained state -/

/-- What a pruned object leaves behind. `prunedAt` is the instant of the
    prune; `expiresAt` is the earliest instant the `expire` step may
    remove the tombstone. -/
structure Tombstone where
  id        : Ident
  prunedAt  : Nat
  expiresAt : Nat
  deriving Repr, DecidableEq

instance : BEq Tombstone := instBEqOfDecidableEq

structure RState where
  base  : ServerState     := {}
  tombs : List Tombstone  := []
  deriving Repr, BEq

def RState.init : RState := {}

/-- A tombstone is LIVE at `now` until its expiry. Liveness is what
    `tombstone_kept` guarantees; it is not what `buried` tests. -/
def Tombstone.live (t : Tombstone) (now : Nat) : Bool :=
  now < t.expiresAt

/-- The id has a tombstone — live or past expiry but not yet swept. -/
def buried (r : RState) (id : Ident) : Bool :=
  r.tombs.any (·.id == id)

/-! ## Pruning -/

/-- Some task still holds `id` as a trigger it has not consumed. Read on
    the STORED rows, not projected: a stored resume on a task whose
    promise has since passed its deadline is conservative to honour. -/
def resumedBy (s : ServerState) (id : Ident) : Bool :=
  s.objects.any fun q => q.task.any (·.resumes.contains id)

/-- Every clause on the PROJECTED object, because that is the object a
    read at `now` would serve: a promise stored pending whose deadline
    has passed is settled for every observer, and is prunable on the
    same terms as one a client settled.

    The task clause is vacuous on the projection — `TaskObject.view`
    fulfils the task of every settled promise — and is kept so that the
    statement reads as the implementation's check does. -/
def prunable (π : Policy) (s : ServerState) (o : Object) (now : Nat) : Bool :=
  let o' := o.project now
  let p  := o'.promise
  p.state != .pending
    && (match p.settledAt, π.age? p.tags with
        | some t, some a => t + a ≤ now
        | _,      _      => false)
    && p.callbacks.isEmpty
    && p.listeners.isEmpty
    && o'.task.all (·.state == .fulfilled)
    && !resumedBy s o.id

/-- Strike `id` from an object's `callbacks`. Every awaiter edge pointing
    at a pruned id goes with it, so no drain ever tries to resume an id
    the store no longer holds. -/
def dropAwaiter (id : Ident) (q : Object) : Object :=
  { q with promise := { q.promise with callbacks := q.promise.callbacks.filter (· != id) } }

def Tombstone.ofPrune (π : Policy) (id : Ident) (now : Nat) : Tombstone :=
  { id := id, prunedAt := now, expiresAt := now + π.horizon }

/-- The prune step, at one id. Present and prunable: the object goes, its
    id leaves every `callbacks` list, and a tombstone replaces any other
    tombstone at that id. Anything else: no change. The outbox and the
    schedules are not touched. -/
def prune (π : Policy) (id : Ident) (now : Nat) (r : RState) : RState :=
  match r.base.objects.find? (·.id == id) with
  | none   => r
  | some o =>
      if prunable π r.base o now then
        { base  := { r.base with
                     objects := (r.base.objects.filter (·.id != id)).map (dropAwaiter id) },
          tombs := Tombstone.ofPrune π id now :: r.tombs.filter (·.id != id) }
      else
        r

/-- The sweep. Drops every tombstone whose expiry has come. It may fire
    LATE — at any instant after the expiry — which is why a tombstone
    lives at least, not exactly, until `expiresAt`. -/
def expire (now : Nat) (r : RState) : RState :=
  { r with tombs := r.tombs.filter (·.live now) }

/-! ## Requests against a tombstone -/

/-- The ids a request is ABOUT: every id it reads or may create. A
    request with a tombstoned subject is answered `expired`.

    `taskFulfill` lists only `req.id`: the handler settles the task's
    own promise and never reads `req.action.id`. `taskHeartbeat` lists
    nothing — a heartbeat against a missing task is already a no-op,
    and answering one batch `expired` for one stale ref would refuse the
    live ones in it. -/
def subjects : Request → List Ident
  | .promiseGet              r => [r.id]
  | .promiseCreate           r => [r.id]
  | .promiseSettle           r => [r.id]
  | .promiseRegisterCallback r => [r.awaited, r.awaiter]
  | .promiseRegisterListener r => [r.awaited]
  | .promiseSearch           _ => []
  | .scheduleGet             _ => []
  | .scheduleCreate          _ => []
  | .scheduleDelete          _ => []
  | .scheduleSearch          _ => []
  | .taskGet                 r => [r.id]
  | .taskCreate              r => [r.action.id]
  | .taskAcquire             r => [r.id]
  | .taskFence               r => [r.id, r.action.targetId]
  | .taskHeartbeat           _ => []
  | .taskSuspend             r => r.id :: r.actions.map (·.awaited)
  | .taskFulfill             r => [r.id]
  | .taskRelease             r => [r.id]
  | .taskHalt                r => [r.id]
  | .taskContinue            r => [r.id]
  | .taskSearch              _ => []

/-- The first tombstoned subject of a step, if it is a request. Internal
    steps and `idle` are never refused: they are the server's own, and
    the filter behind the step is what stops them re-creating an id. -/
def refusal (r : RState) : Step → Option Ident
  | .external rq => (subjects rq).find? (buried r ·)
  | _            => none

/-! ## The retained step -/

inductive RResponse
  | base    (r : Response)
  | expired (id : Ident)
  deriving Repr, BEq

inductive RStep
  | base   (st : Step)
  | prune  (id : Ident)
  | expire
  deriving Repr

/-- The status line of a response, for the corpus and for the wire:
    `expired` is 410, `silent` is 0. -/
def responseStatus : Response → Nat
  | .promiseGet              r => r.status
  | .promiseCreate           r => r.status
  | .promiseSettle           r => r.status
  | .promiseRegisterCallback r => r.status
  | .promiseRegisterListener r => r.status
  | .promiseSearch           r => r.status
  | .scheduleGet             r => r.status
  | .scheduleCreate          r => r.status
  | .scheduleDelete          r => r.status
  | .scheduleSearch          r => r.status
  | .taskGet                 r => r.status
  | .taskCreate              r => r.status
  | .taskAcquire             r => r.status
  | .taskFence               r => r.status
  | .taskHeartbeat           r => r.status
  | .taskSuspend             r => r.status
  | .taskFulfill             r => r.status
  | .taskRelease             r => r.status
  | .taskHalt                r => r.status
  | .taskContinue            r => r.status
  | .taskSearch              r => r.status
  | .silent                    => 0

def RResponse.status : RResponse → Nat
  | .base r    => responseStatus r
  | .expired _ => 410

/-- No object survives at a tombstoned id. For a request that passed the
    guard this removes nothing (every id a request can create is one of
    its subjects — `external_births_are_subjects`); for an internal step
    it is what refuses a schedule's `createIfAbsent` at a pruned id. -/
def scrub (r : RState) (s : ServerState) : ServerState :=
  { s with objects := s.objects.filter fun o => !buried r o.id }

def rstepOf (mat : Bool) (π : Policy) : RStep → Nat → RState → RResponse × RState
  | .base st, now, r =>
      match refusal r st with
      | some id => (.expired id, r)
      | none    =>
          let (res, s') := stepOf mat st now r.base
          (.base res, { r with base := scrub r s' })
  | .prune id, now, r => (.base .silent, prune π id now r)
  | .expire,   now, r => (.base .silent, expire now r)

def runR (mat : Bool) (π : Policy) :
    List (RStep × Nat) → RState → List RResponse × RState
  | [],           r => ([], r)
  | (st, n) :: w, r =>
      let (x, r')   := rstepOf mat π st n r
      let (xs, r'') := runR mat π w r'
      (x :: xs, r'')

/-! ## Properties

Written in the catalogue's two shapes (`Properties.Property`), over
`RState` instead of `ServerState`. They are a separate list and do not
touch `Properties.catalogue`: the old entries still apply to `r.base`,
except the few pruning breaks on purpose, which
`04-theorems/retention.lean` names. -/

inductive RProperty where
  | state (f : Nat → RState → Bool)
  | trans (f : Nat → RState → RState → Bool)

structure RNamed where
  name     : String
  property : RProperty

/-- **No resurrection.** No object has the id of any tombstone in the
    store. Stronger than design 74's wording — it covers a tombstone
    past its expiry and not yet swept, which is what the guard enforces
    — and `no_resurrection_within_horizon` is the wording itself. -/
def no_resurrection (_now : Nat) (r : RState) : Bool :=
  r.tombs.all fun t => !r.base.objects.any (·.id == t.id)

/-- Design 74, as written: no pruned id is re-created while its
    tombstone is live. Implied by `no_resurrection`; stated so the
    design's sentence has a name. -/
def no_resurrection_within_horizon (now : Nat) (r : RState) : Bool :=
  r.tombs.all fun t => !t.live now || !r.base.objects.any (·.id == t.id)

/-- **A tombstone lives at least until its expiry.** Every tombstone
    live at `now` in the pre-state is in the post-state, unchanged. One
    past its expiry may or may not have been swept. -/
def tombstone_kept (now : Nat) (a b : RState) : Bool :=
  a.tombs.all fun t => !t.live now || b.tombs.contains t

/-- **Nothing leaves without a tombstone.** Every id present before a
    step and absent after it is tombstoned after it.

    The retained machine's replacement for the catalogue's
    `monotone_promise_set_grows`, which pruning breaks on purpose: the
    object set may now shrink, but only by leaving a name behind. -/
def pruned_leaves_tombstone (_now : Nat) (a b : RState) : Bool :=
  a.base.objects.all fun o => b.base.objects.any (·.id == o.id) || buried b o.id

/-- At most one tombstone per id. Prune replaces, never adds a second. -/
def well_formed_tombstone_ids_unique (_now : Nat) (r : RState) : Bool :=
  (r.tombs.map (·.id)).eraseDups.length == r.tombs.length

/-- Every tombstone was written in the past. -/
def well_formed_tombstone_pruned_at_lte_now (now : Nat) (r : RState) : Bool :=
  r.tombs.all fun t => t.prunedAt ≤ now

/-- Every tombstone expires exactly one horizon after its prune — so,
    with `tombstone_kept`, it lives at least until `prunedAt + horizon`.
    Parametric in the policy, which is why it is not in `rcatalogue`. -/
def well_formed_tombstone_expiry_is_horizon (π : Policy) (_now : Nat) (r : RState) : Bool :=
  r.tombs.all fun t => t.expiresAt == t.prunedAt + π.horizon

def rcatalogue : List RNamed :=
  [ { name := "no_resurrection",                 property := .state no_resurrection },
    { name := "no_resurrection_within_horizon",  property := .state no_resurrection_within_horizon },
    { name := "well_formed_tombstone_ids_unique", property := .state well_formed_tombstone_ids_unique },
    { name := "well_formed_tombstone_pruned_at_lte_now",
      property := .state well_formed_tombstone_pruned_at_lte_now },
    { name := "tombstone_kept",                  property := .trans tombstone_kept },
    { name := "pruned_leaves_tombstone",         property := .trans pruned_leaves_tombstone } ]

/-- The retained catalogue at one step, `legalAt`'s shape. -/
def rlegalAt (now : Nat) (a b : RState) : Bool :=
  rcatalogue.all fun l =>
    match l.property with
    | .state f => f now a && f now b
    | .trans f => f now a b

def rfailures (now : Nat) (a b : RState) : List String :=
  rcatalogue.filterMap fun l =>
    match l.property with
    | .state f => if f now a && f now b then none else some l.name
    | .trans f => if f now a b          then none else some l.name

end Retention
