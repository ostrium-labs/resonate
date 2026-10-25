import «04-theorems».«corpus»
import «02-abstract».«properties»
import «02-abstract».«retention»

/-!  # Retention, proved and swept (FW5)

What `02-abstract/retention.lean` claims about the retained machine,
in two tiers.

## Proved, for every step

  * `no_resurrection_step` — every step preserves `no_resurrection`.
    Unconditionally for a request that passed the guard (the filter
    behind it re-establishes the property from nothing); from the
    pre-state for the rest, none of which adds an object.
  * `tombstone_kept_step` — a live tombstone survives every step,
    unchanged. Needs `no_resurrection` before: a prune REPLACES any
    tombstone at its id, and it is the property that says there is none
    to replace (a prune needs an object at the id).
  * `pruned_leaves_tombstone_step` — an id leaves the store only by
    leaving a tombstone. Unconditional.
  * `expired_is_definite` — a request with a tombstoned subject is
    answered `expired` and changes nothing.
  * `prune_off`, `runR_conservative` — with no tombstones and
    retention off, the retained machine IS the old one.
    `runR_conservative` rests on `rstep_base_conservative`, which is
    still `sorry` (TODO(FW5), see there) and backed by
    `conservative_battery`.

The clock does not appear in the first claim. `no_resurrection` is
about presence, not liveness (see the header of the definitions), so a
state that satisfies it satisfies it at every instant;
`no_resurrection_within_horizon_of` turns it into design 74's sentence.

## Stated with `sorry`

  * `external_births_are_subjects` — every id a request can create is
    one of its `subjects`. It is what makes the filter behind a request
    a no-op, and so what makes `expired` the ONLY observable change a
    tombstone causes on the request path. Backed by `births_sweep`.
  * `rstep_base_conservative` — TODO(FW5), one goal left open.
  * `base_catalogue_on_retained` — TODO(FW5): `prunedBreaks` is not
    filled in yet, and with it empty the statement is false.

## Decided

The scripts at the bottom: every retained property along every step,
the responses each step must give, and the base catalogue on `r.base`
with the entries pruning breaks on purpose named — `prunedBreaks` —
and every one of them shown to break on a corpus script, so the list
is exact rather than generous. -/

namespace Retention

open ServerModel (Ident)
open AbstractModel (Object ServerState Effect applyAll)
open Abstraction (Step stepOf runFin oid extTags tgtTags)

/-! ## Reading the Bool properties as propositions -/

theorem buried_iff (r : RState) (id : Ident) :
    buried r id = true ↔ ∃ t ∈ r.tombs, t.id = id := by
  simp [buried]

theorem no_resurrection_iff (n : Nat) (r : RState) :
    no_resurrection n r = true ↔
      ∀ t ∈ r.tombs, ∀ o ∈ r.base.objects, o.id ≠ t.id := by
  simp [no_resurrection]

theorem tombstone_kept_iff (n : Nat) (a b : RState) :
    tombstone_kept n a b = true ↔
      ∀ t ∈ a.tombs, t.live n = true → t ∈ b.tombs := by
  simp only [tombstone_kept, List.all_eq_true, Bool.or_eq_true, Bool.not_eq_true',
             List.contains_iff_mem]
  constructor
  · intro h t ht hl
    rcases h t ht with h | h
    · rw [hl] at h; cases h
    · exact h
  · intro h t ht
    cases hl : t.live n
    · exact Or.inl rfl
    · exact Or.inr (h t ht hl)

theorem pruned_leaves_tombstone_iff (n : Nat) (a b : RState) :
    pruned_leaves_tombstone n a b = true ↔
      ∀ o ∈ a.base.objects,
        (∃ q ∈ b.base.objects, q.id = o.id) ∨ buried b o.id = true := by
  simp [pruned_leaves_tombstone]

/-! ## The one frame fact: a base step never removes an id

The same fact as `Frame.monotone_promise_set_grows_step`, reproved here
in three lines so this file depends on the corpus and nothing heavier. -/

theorem apply_keeps_id (f : Effect) (s : ServerState) (id : Ident)
    (h : ∃ o ∈ s.objects, o.id = id) : ∃ o ∈ (f.apply s).objects, o.id = id := by
  obtain ⟨o, ho, hid⟩ := h
  cases f with
  | setPromise i p =>
      by_cases hi : o.id = i
      · refine ⟨AbstractModel.Object.withPromise i p (s.objects.find? (·.id == i)),
                List.mem_cons_self, ?_⟩
        cases hf : s.objects.find? (·.id == i) with
        | none   => simp [AbstractModel.Object.withPromise]; exact hi.symm.trans hid
        | some q =>
            have hq := List.find?_some hf
            simp at hq
            simp [AbstractModel.Object.withPromise, hq, ← hid, hi]
      · exact ⟨o, List.mem_cons_of_mem _ (List.mem_filter.mpr ⟨ho, by simp [hi]⟩), hid⟩
  | setTask i t =>
      refine ⟨if o.id == i then { o with task := some t } else o, ?_, ?_⟩
      · exact List.mem_map.mpr ⟨o, ho, rfl⟩
      · split <;> exact hid
  | setSchedule _ | delSchedule _ | setMessage _ _ => exact ⟨o, ho, hid⟩

theorem applyAll_keeps_id :
    ∀ (w : List Effect) (s : ServerState) (id : Ident),
      (∃ o ∈ s.objects, o.id = id) → ∃ o ∈ (applyAll s w).objects, o.id = id
  | [],      _, _,  h => h
  | f :: fs, s, id, h => applyAll_keeps_id fs (f.apply s) id (apply_keeps_id f s id h)

theorem stepOf_keeps_id (mat : Bool) (st : Step) (now : Nat) (s : ServerState) (id : Ident)
    (h : ∃ o ∈ s.objects, o.id = id) : ∃ o ∈ (stepOf mat st now s).2.objects, o.id = id :=
  applyAll_keeps_id _ s id h

/-! ## The four step cases, as equations -/

theorem rstep_refused (mat : Bool) (π : Policy) (st : Step) (now : Nat) (r : RState)
    (id : Ident) (h : refusal r st = some id) :
    rstepOf mat π (.base st) now r = (.expired id, r) := by
  simp [rstepOf, h]

theorem rstep_admitted (mat : Bool) (π : Policy) (st : Step) (now : Nat) (r : RState)
    (h : refusal r st = none) :
    rstepOf mat π (.base st) now r =
      (.base (stepOf mat st now r.base).1, { r with base := scrub r (stepOf mat st now r.base).2 }) := by
  simp [rstepOf, h]

/-- The prune step, by cases. Either the step changes nothing, or there
    is an object `o` at `id`, it is prunable, and the step is the
    deletion. -/
theorem prune_cases (π : Policy) (id : Ident) (now : Nat) (r : RState) :
    prune π id now r = r ∨
      ∃ o ∈ r.base.objects, o.id = id ∧
        prune π id now r =
          { base  := { r.base with
                       objects := (r.base.objects.filter (·.id != id)).map (dropAwaiter id) },
            tombs := Tombstone.ofPrune π id now :: r.tombs.filter (·.id != id) } := by
  unfold prune
  cases hf : r.base.objects.find? (·.id == id) with
  | none   => exact Or.inl rfl
  | some o =>
      simp only
      split
      · have hq := List.find?_some hf
        simp at hq
        exact Or.inr ⟨o, List.mem_of_find?_eq_some hf, hq, rfl⟩
      · exact Or.inl rfl

/-! ## `no_resurrection` is an invariant -/

theorem no_resurrection_scrub (n : Nat) (r : RState) (s : ServerState) :
    no_resurrection n { r with base := scrub r s } = true := by
  rw [no_resurrection_iff]
  intro t ht o ho hid
  simp only [scrub, List.mem_filter, Bool.not_eq_true'] at ho
  have : buried r o.id = true := (buried_iff r o.id).mpr ⟨t, ht, hid.symm⟩
  rw [this] at ho
  cases ho.2

theorem no_resurrection_step (mat : Bool) (π : Policy) (st : RStep) (now n : Nat)
    (r : RState) (h : no_resurrection n r = true) :
    no_resurrection n (rstepOf mat π st now r).2 = true := by
  cases st with
  | base st =>
      cases hr : refusal r st with
      | some id => rw [rstep_refused mat π st now r id hr]; exact h
      | none    => rw [rstep_admitted mat π st now r hr]; exact no_resurrection_scrub n r _
  | prune id =>
      show no_resurrection n (prune π id now r) = true
      rcases prune_cases π id now r with he | ⟨_, _, _, he⟩
      · rw [he]; exact h
      · rw [he, no_resurrection_iff]
        rw [no_resurrection_iff] at h
        intro t ht q hq hid
        simp only [List.mem_map, List.mem_filter, bne_iff_ne, ne_eq] at hq
        obtain ⟨q', ⟨hq', hne⟩, rfl⟩ := hq
        simp only [dropAwaiter] at hid
        simp only [List.mem_cons, List.mem_filter, bne_iff_ne, ne_eq] at ht
        rcases ht with rfl | ⟨ht, _⟩
        · exact hne hid
        · exact h t ht q' hq' hid
  | expire =>
      show no_resurrection n (expire now r) = true
      rw [no_resurrection_iff] at h ⊢
      intro t ht o ho
      simp only [expire, List.mem_filter] at ht
      exact h t ht.1 o ho

/-- Design 74's sentence, from the stronger one: no pruned id is present
    while its tombstone is live. -/
theorem no_resurrection_within_horizon_of (n : Nat) (r : RState)
    (h : no_resurrection n r = true) : no_resurrection_within_horizon n r = true := by
  rw [no_resurrection_iff] at h
  simp only [no_resurrection_within_horizon, List.all_eq_true, Bool.or_eq_true,
             Bool.not_eq_true', List.any_eq_false, beq_iff_eq]
  intro t ht
  exact Or.inr (fun o ho hid => h t ht o ho hid)

/-! ## A live tombstone is kept -/

theorem tombstone_kept_step (mat : Bool) (π : Policy) (st : RStep) (now : Nat)
    (r : RState) (h : no_resurrection now r = true) :
    tombstone_kept now r (rstepOf mat π st now r).2 = true := by
  rw [tombstone_kept_iff]
  intro t ht _
  cases st with
  | base st =>
      cases hr : refusal r st with
      | some id => rw [rstep_refused mat π st now r id hr]; exact ht
      | none    => rw [rstep_admitted mat π st now r hr]; exact ht
  | prune id =>
      show t ∈ (prune π id now r).tombs
      rcases prune_cases π id now r with he | ⟨o, ho, hoid, he⟩
      · rw [he]; exact ht
      · rw [he]
        rw [no_resurrection_iff] at h
        have hne : t.id ≠ id := fun hti => h t ht o ho (hoid.trans hti.symm)
        exact List.mem_cons_of_mem _ (List.mem_filter.mpr ⟨ht, by simp [hne]⟩)
  | expire =>
      show t ∈ (expire now r).tombs
      simp only [expire, List.mem_filter]
      exact ⟨ht, by assumption⟩

/-! ## Nothing leaves without a tombstone -/

theorem pruned_leaves_tombstone_step (mat : Bool) (π : Policy) (st : RStep) (now n : Nat)
    (r : RState) : pruned_leaves_tombstone n r (rstepOf mat π st now r).2 = true := by
  rw [pruned_leaves_tombstone_iff]
  intro o ho
  cases st with
  | base st =>
      cases hr : refusal r st with
      | some id =>
          rw [rstep_refused mat π st now r id hr]
          exact Or.inl ⟨o, ho, rfl⟩
      | none =>
          rw [rstep_admitted mat π st now r hr]
          obtain ⟨q, hq, hqid⟩ := stepOf_keeps_id mat st now r.base o.id ⟨o, ho, rfl⟩
          cases hb : buried r o.id
          · refine Or.inl ⟨q, ?_, hqid⟩
            simp only [scrub, List.mem_filter, Bool.not_eq_true']
            exact ⟨hq, hqid ▸ hb⟩
          · exact Or.inr hb
  | prune id =>
      show (∃ q ∈ (prune π id now r).base.objects, q.id = o.id) ∨ buried (prune π id now r) o.id = true
      rcases prune_cases π id now r with he | ⟨_, _, _, he⟩
      · rw [he]; exact Or.inl ⟨o, ho, rfl⟩
      · rw [he]
        by_cases hid : o.id = id
        · refine Or.inr ((buried_iff _ _).mpr ⟨Tombstone.ofPrune π id now, List.mem_cons_self, ?_⟩)
          simp [Tombstone.ofPrune, hid]
        · refine Or.inl ⟨dropAwaiter id o, ?_, rfl⟩
          exact List.mem_map.mpr ⟨o, List.mem_filter.mpr ⟨ho, by simp [hid]⟩, rfl⟩
  | expire =>
      exact Or.inl ⟨o, ho, rfl⟩

/-! ## A tombstoned subject is a definite answer -/

/-- **`expired_is_definite`.** A request naming a tombstoned id, anywhere
    in its subjects, is answered `expired` — naming the FIRST such
    subject — and leaves the retained state exactly as it was. Nothing
    is materialised, nothing is written to the outbox. -/
theorem expired_is_definite (mat : Bool) (π : Policy) (rq : Equivalence.Request) (now : Nat)
    (r : RState) (id : Ident) (hmem : id ∈ subjects rq) (hb : buried r id = true) :
    ∃ id', buried r id' = true ∧
      rstepOf mat π (.base (.external rq)) now r = (.expired id', r) := by
  cases hf : (subjects rq).find? (buried r ·) with
  | none =>
      have := List.find?_eq_none.mp hf id hmem
      rw [hb] at this
      cases this rfl
  | some id' =>
      refine ⟨id', List.find?_some hf, ?_⟩
      exact rstep_refused mat π _ now r id' (by simp [refusal, hf])

/-! ## Conservativity -/

/-- With no tombstones a base step of the retained machine is the base
    step of the old machine: the guard never fires and the filter keeps
    every object. The policy does not appear — it is read only by
    `prune`. -/
theorem rstep_base_conservative (mat : Bool) (π : Policy) (st : Step) (now : Nat)
    (s : ServerState) :
    rstepOf mat π (.base st) now { base := s, tombs := [] } =
      (.base (stepOf mat st now s).1, { base := (stepOf mat st now s).2, tombs := [] }) := by
  -- TODO(FW5): the proof below left one goal open under Lean 4.34.1
  -- (`simp [scrub, buried]` does not close the record equation). The
  -- argument: with `tombs = []`, `buried _ id = [].any _ = false`, so
  -- `refusal` is `none` (`List.find?` of a false predicate) and `scrub`
  -- filters by `fun _ => !false`, which is `List.filter_true`. Checked on
  -- the corpus by `conservative_battery`.
  sorry

/-- Retention off: no rule, so no promise is ever prunable and the prune
    step is the identity. -/
theorem prune_off (id : Ident) (now : Nat) (r : RState) : prune Policy.off id now r = r := by
  rcases prune_cases Policy.off id now r with he | ⟨o, _, _, he⟩
  · exact he
  · unfold prune at he ⊢
    split
    · rfl
    · rename_i o' _
      have : prunable Policy.off r.base o' now = false := by
        simp [prunable, Policy.age?, Policy.off]
      simp [this]

/-- The lift of an old script into the retained alphabet. -/
def lift (w : List (Step × Nat)) : List (RStep × Nat) :=
  w.map fun (st, n) => (.base st, n)

/-- **Conservativity, for whole runs.** A script of old steps, run on the
    retained machine from a state with no tombstones, gives the old
    machine's responses and the old machine's final state, with no
    tombstone. -/
theorem runR_conservative (mat : Bool) (π : Policy) :
    ∀ (w : List (Step × Nat)) (s : ServerState),
      runR mat π (lift w) { base := s, tombs := [] } =
        ((runFin mat w s).1.map .base, { base := (runFin mat w s).2, tombs := [] })
  | [],           _ => rfl
  | (st, n) :: w, s => by
      simp only [lift, List.map_cons] at *
      simp only [runR, runFin, rstep_base_conservative]
      have ih := runR_conservative mat π w (stepOf mat st n s).2
      simp only [lift] at ih
      rw [ih]
      rfl

/-- `no_resurrection` along a whole run. -/
theorem runR_no_resurrection (mat : Bool) (π : Policy) (n : Nat) :
    ∀ (w : List (RStep × Nat)) (r : RState),
      no_resurrection n r = true → no_resurrection n (runR mat π w r).2 = true
  | [],           _, h => h
  | (st, k) :: w, r, h =>
      runR_no_resurrection mat π n w _ (no_resurrection_step mat π st k n r h)

/-! ## Births are subjects -/

/-- The ids a step adds to the store. -/
def births (a b : ServerState) : List Ident :=
  (b.objects.map (·.id)).filter fun i => !a.objects.any (·.id == i)

/-- **Every id a request can create is one of its subjects.** So the
    filter behind an admitted request removes nothing that request
    wrote, and — given `no_resurrection` before — nothing at all.

    Stated with `sorry`, so the build names it on every run. The
    argument is short and per handler. An object is born only by
    `Effect.setPromise` at an id `find?` misses (`setTask` is a map, and
    the other three effects do not touch `objects`). Every `setPromise`
    a handler emits is at an id it just read and found — the
    materialising write in `materialise`, `setSettled`, the
    `addCallback`/`addListener` writes, `registerAwaited` — except the
    three creation sites: `createPromise` from `promiseCreate` (at
    `req.id`) and from `taskFence`'s `.create` arm (at
    `req.action.targetId`), and both arms of `taskCreate` (at
    `req.action.id`). Those are exactly the subjects listed for those
    three requests. Backed by `births_sweep`. -/
theorem external_births_are_subjects (mat : Bool) (rq : Equivalence.Request) (now : Nat)
    (s : ServerState) :
    (births s (stepOf mat (.external rq) now s).2).all (subjects rq).contains = true := sorry

/-! ## The corpus

Policies first, then scripts. Every script stays in origin `"o"`
(`oid`), every instant is chosen so the clock never runs backwards. -/

/-- Every promise, kept 50 ms after settling; tombstones for 1000 ms. -/
def πAll : Policy := { rules := [{ age := 50 }], horizon := 1000 }

/-- Only external promises (tag value starting `"t"`, which `"true"`
    does), 50 ms; tombstones for 1000 ms. -/
def πExt : Policy :=
  { rules := [{ tag := some "resonate:external", prefixes := ["t"], age := 50 }],
    horizon := 1000 }

/-- A rule whose prefix the corpus's targets (`"w1"`) never start with. -/
def πNoMatch : Policy :=
  { rules := [{ tag := some "resonate:target", prefixes := ["w2"], age := 0 }],
    horizon := 1000 }

/-- The minimum wins: a catch-all at 500 and an external rule at 50. -/
def πTwo : Policy :=
  { rules := [{ age := 500 }, { tag := some "resonate:external", age := 50 }],
    horizon := 1000 }

theorem age_all       : πAll.age? extTags = some 50               := by decide
theorem age_ext       : πExt.age? extTags = some 50               := by decide
theorem age_ext_tgt   : πExt.age? tgtTags = none                  := by decide
theorem age_nomatch   : πNoMatch.age? tgtTags = none              := by decide
theorem age_min       : πTwo.age? extTags = some 50               := by decide
theorem age_min_other : πTwo.age? tgtTags = some 500              := by decide
theorem age_off       : Policy.off.age? extTags = none            := by decide
theorem prefix_any    : ({ tag := some "resonate:target", age := 0 } : Rule).selects tgtTags = true
                        := by decide

/-- Create, settle, prune too early, prune on time; then every request at
    the id is refused, the sweep is too early by one instant, then on
    time, and the id can be created again. -/
def rPrune : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 200),
    (.prune (oid "a"), 249),
    (.prune (oid "a"), 250),
    (.base (.external (.promiseGet { id := oid "a" })), 260),
    (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 270),
    (.base (.external (.promiseRegisterListener { awaited := oid "a", address := "https://l" })), 280),
    (.expire, 1249),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 1249),
    (.expire, 1250),
    (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 1300),
    (.base (.external (.promiseGet { id := oid "a" })), 1310) ]

/-- A tombstone past its expiry and not yet swept still refuses. -/
def rLateSweep : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 200),
    (.prune (oid "a"), 300),
    (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 5000),
    (.expire, 5000),
    (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 5000) ]

/-- Pending is never pruned. Past its deadline it is, without ever being
    materialised — the projection is what the prune judges. -/
def rPending : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 400, param := {}, tags := extTags })), 100),
    (.prune (oid "a"), 399),
    (.prune (oid "a"), 449),
    (.prune (oid "a"), 450),
    (.base (.external (.promiseGet { id := oid "a" })), 460) ]

/-- A registered listener holds the promise until the drain consumes it. -/
def rListener : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.promiseRegisterListener { awaited := oid "a", address := "https://l" })), 110),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 200),
    (.prune (oid "a"), 300),
    (.base (.internal (.listener { awaited := oid "a", address := "https://l" })), 300),
    (.prune (oid "a"), 300) ]

/-- A callback holds the awaited promise until it is drained; then the
    awaiter's `resumes` holds it until the awaiter is dispatched again. -/
def rCallback : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 10000, param := {}, tags := tgtTags } })), 100),
    (.base (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }] })), 120),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 200),
    (.prune (oid "a"), 300),
    (.base (.internal (.callback { awaited := oid "a", awaiter := oid "x" })), 300),
    (.prune (oid "a"), 300),
    (.base (.external (.taskAcquire { id := oid "x", version := 1, pid := "p1", ttl := 1000 })), 310),
    (.prune (oid "a"), 310),
    (.base (.external (.promiseGet { id := oid "a" })), 320),
    (.base (.external (.taskGet { id := oid "x" })), 320) ]

/-- A targeted promise and its task go together, once the task is
    fulfilled — and every task door and the callback door then refuse. -/
def rTargeted : List (RStep × Nat) :=
  [ (.base (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 10000, param := {}, tags := tgtTags } })), 100),
    (.base (.external (.promiseCreate { id := oid "b", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.prune (oid "x"), 150),
    (.base (.external (.taskFulfill { id := oid "x", version := 1, action := { id := oid "x", state := .resolved, value := {} } })), 200),
    (.prune (oid "x"), 249),
    (.prune (oid "x"), 250),
    (.base (.external (.taskGet { id := oid "x" })), 260),
    (.base (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 10000, param := {}, tags := tgtTags } })), 270),
    (.base (.external (.promiseRegisterCallback { awaited := oid "b", awaiter := oid "x" })), 280),
    (.base (.external (.promiseGet { id := oid "b" })), 290) ]

/-- An awaiter pruned while still listed in a pending promise's
    callbacks: the edge goes with it. `x` suspends on `a` and `b`, `a`
    wakes it, it finishes, and `b` is still pending with `x` in its
    callbacks. -/
def rAwaiter : List (RStep × Nat) :=
  [ (.base (.external (.promiseCreate { id := oid "a", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.promiseCreate { id := oid "b", timeoutAt := 10000, param := {}, tags := extTags })), 100),
    (.base (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 10000, param := {}, tags := tgtTags } })), 100),
    (.base (.external (.taskSuspend { id := oid "x", version := 1, actions := [{ awaited := oid "a", awaiter := oid "x" }, { awaited := oid "b", awaiter := oid "x" }] })), 120),
    (.base (.external (.promiseSettle { id := oid "a", state := .resolved, value := {} })), 200),
    (.base (.internal (.callback { awaited := oid "a", awaiter := oid "x" })), 200),
    (.base (.external (.taskAcquire { id := oid "x", version := 1, pid := "p1", ttl := 1000 })), 210),
    (.base (.external (.taskFulfill { id := oid "x", version := 2, action := { id := oid "x", state := .resolved, value := {} } })), 220),
    (.prune (oid "x"), 300) ]

/-- A tag-prefix rule that matches nothing never prunes. -/
def rNoMatch : List (RStep × Nat) :=
  [ (.base (.external (.taskCreate { pid := "p0", ttl := 1000, action := { id := oid "x", timeoutAt := 10000, param := {}, tags := tgtTags } })), 100),
    (.base (.external (.taskFulfill { id := oid "x", version := 1, action := { id := oid "x", state := .resolved, value := {} } })), 200),
    (.prune (oid "x"), 5000),
    (.base (.external (.taskGet { id := oid "x" })), 5000) ]

/-! ### Running them -/

def statesR (mat : Bool) (π : Policy) :
    List (RStep × Nat) → RState → List (Nat × RState × RState)
  | [],           _ => []
  | (st, n) :: w, r =>
      let (_, r') := rstepOf mat π st n r
      (n, r, r') :: statesR mat π w r'

/-- Every retained property at every step, both read disciplines. -/
def rCheck (π : Policy) (w : List (RStep × Nat)) : Bool :=
  (statesR true π w RState.init ++ statesR false π w RState.init).all
    fun (n, a, b) => rlegalAt n a b && well_formed_tombstone_expiry_is_horizon π n b

def codes (mat : Bool) (π : Policy) (w : List (RStep × Nat)) : List Nat :=
  (runR mat π w RState.init).1.map (·.status)

def final (π : Policy) (w : List (RStep × Nat)) : RState :=
  (runR true π w RState.init).2

def has (r : RState) (s : String) : Bool :=
  r.base.objects.any (·.id == oid s)

set_option maxRecDepth 100000
set_option maxHeartbeats 4000000

theorem rPrune_props : rCheck πAll rPrune = true := by decide
theorem rPrune_codes :
    codes true πAll rPrune = [200, 200, 0, 0, 410, 410, 410, 0, 410, 0, 200, 200] := by decide
theorem rPrune_codes_view :
    codes false πAll rPrune = [200, 200, 0, 0, 410, 410, 410, 0, 410, 0, 200, 200] := by decide
theorem rPrune_early_keeps : has (final πAll (rPrune.take 3)) "a" = true := by decide
theorem rPrune_gone :
    ((final πAll (rPrune.take 4)).base.objects.isEmpty
      && ((final πAll (rPrune.take 4)).tombs
           == [{ id := oid "a", prunedAt := 250, expiresAt := 1250 }])) = true := by decide
theorem rPrune_refusal_writes_nothing :
    (final πAll (rPrune.take 7) == final πAll (rPrune.take 4)) = true := by decide
theorem rPrune_back : (has (final πAll rPrune) "a" && (final πAll rPrune).tombs.isEmpty) = true := by
  decide

theorem rLateSweep_props : rCheck πAll rLateSweep = true := by decide
theorem rLateSweep_codes : codes true πAll rLateSweep = [200, 200, 0, 410, 0, 200] := by decide

theorem rPending_props : rCheck πAll rPending = true := by decide
theorem rPending_codes : codes true πAll rPending = [200, 0, 0, 0, 410] := by decide
theorem rPending_holds : has (final πAll (rPending.take 3)) "a" = true := by decide

theorem rListener_props : rCheck πAll rListener = true := by decide
theorem rListener_holds : has (final πAll (rListener.take 4)) "a" = true := by decide
theorem rListener_released : has (final πAll rListener) "a" = false := by decide

theorem rCallback_props : rCheck πAll rCallback = true := by decide
theorem rCallback_holds_on_callback : has (final πAll (rCallback.take 5)) "a" = true := by decide
theorem rCallback_holds_on_resume : has (final πAll (rCallback.take 7)) "a" = true := by decide
theorem rCallback_released : has (final πAll (rCallback.take 9)) "a" = false := by decide
theorem rCallback_codes :
    codes true πAll rCallback = [200, 200, 200, 200, 0, 0, 0, 200, 0, 410, 200] := by decide

theorem rTargeted_props : rCheck πAll rTargeted = true := by decide
theorem rTargeted_codes :
    codes true πAll rTargeted = [200, 200, 0, 200, 0, 0, 410, 410, 410, 200] := by decide
theorem rTargeted_gone :
    (!has (final πAll rTargeted) "x" && has (final πAll rTargeted) "b") = true := by decide

theorem rAwaiter_props : rCheck πAll rAwaiter = true := by decide
/-- `b` was pending with `x` in its callbacks; after `x` is pruned the
    edge is gone and `b` is untouched otherwise. -/
theorem rAwaiter_edge_struck :
    (!has (final πAll rAwaiter) "x"
      && ((final πAll rAwaiter).base.promise? (oid "b")).any (·.callbacks.isEmpty)
      && ((final πAll (rAwaiter.take 8)).base.promise? (oid "b")).any
           (·.callbacks == [oid "x"])) = true := by decide

/-- Only external promises are selected: the same targeted script under
    `πExt` never prunes `x`. -/
theorem rTargeted_ext_keeps : has (final πExt rTargeted) "x" = true := by decide

theorem rNoMatch_props : rCheck πNoMatch rNoMatch = true := by decide
theorem rNoMatch_keeps : codes true πNoMatch rNoMatch = [200, 200, 0, 200] := by decide

/-! ### What pruning breaks in the base catalogue, exactly

The base catalogue, run on `r.base` along every retained script. Each
name in `prunedBreaks` is an entry pruning breaks on purpose, and the
`_witnessed` theorem shows each one does break on some script — so the
list cannot carry an entry nothing violates. Everything else in the
catalogue holds along every script. -/

def retainedScripts : List (Policy × List (RStep × Nat)) :=
  [(πAll, rPrune), (πAll, rLateSweep), (πAll, rPending), (πAll, rListener),
   (πAll, rCallback), (πAll, rTargeted), (πAll, rAwaiter), (πNoMatch, rNoMatch)]

def baseFailures (π : Policy) (w : List (RStep × Nat)) : List String :=
  ((statesR true π w RState.init ++ statesR false π w RState.init).flatMap
    fun (n, a, b) =>
      AbstractModel.Properties.catalogue.filterMap fun l =>
        match l.property with
        | .state f => if f n a.base && f n b.base then none else some l.name
        | .trans f => if f n a.base b.base      then none else some l.name).eraseDups

/-- TODO(FW5): not yet filled in. With the list empty the theorem below
    is FALSE (`decide` refutes it), so it is stated with `sorry` until the
    names are read off `baseFailures` (e.g. by `#eval`) and written here.
    Expected, from reading the entries: `monotone_promise_set_grows`,
    `monotone_task_set_grows`, `preserved_settled_promise_record`, and —
    the ones that are a semantic question for the lead, not just a
    rename — `consistent_outbox_unblock_names_settled_promise` (an
    unblock enqueued by the listener drain outlives the promise it
    names; `rListener` reaches it) and possibly
    `consistent_outbox_execute_names_existing_task` (an execute entry
    for a fulfilled task that is then pruned). -/
def prunedBreaks : List String := []

theorem base_catalogue_on_retained :
    retainedScripts.all (fun (π, w) => (baseFailures π w).all prunedBreaks.contains) = true :=
  sorry

theorem prunedBreaks_witnessed :
    prunedBreaks.all (fun nm => retainedScripts.any fun (π, w) => (baseFailures π w).contains nm)
      = true := by decide

/-! ### Conservativity and births, on the old corpus -/

/-- The hand-built battery of `corpus.lean`. (`Abstraction.battery` lives
    in `properties-check.lean`, which this file does not import.) -/
def oldBattery : List (List (Step × Nat)) :=
  [Abstraction.wLag, Abstraction.b1, Abstraction.b2, Abstraction.b3,
   Abstraction.b4, Abstraction.b5, Abstraction.b6]

/-- Retention off, on the whole battery and both readings: the retained
    run is the old run, response for response and state for state. The
    instance of `runR_conservative` the kernel can see. -/
theorem conservative_battery :
    oldBattery.all (fun w =>
      [true, false].all fun mat =>
        runR mat Policy.off (lift w) RState.init
          == ((runFin mat w AbstractModel.ServerState.init).1.map .base,
              { base := (runFin mat w AbstractModel.ServerState.init).2, tombs := [] }))
      = true := by decide

def birthsOK (mat : Bool) : List (Step × Nat) → ServerState → Bool
  | [],           _ => true
  | (st, n) :: w, s =>
      let s' := (stepOf mat st n s).2
      (match st with
       | .external rq => (births s s').all (subjects rq).contains
       | _            => true)
        && birthsOK mat w s'

/-- `external_births_are_subjects`, on the battery and on every script
    of length ≤ 2 over the adversarial alphabet, both readings. -/
theorem births_sweep :
    (oldBattery ++ (Abstraction.seqsUpToA Abstraction.kernelsResp 2).map
        Abstraction.instantiateA).all
      (fun w => birthsOK true w AbstractModel.ServerState.init
                  && birthsOK false w AbstractModel.ServerState.init) = true := by decide

end Retention
