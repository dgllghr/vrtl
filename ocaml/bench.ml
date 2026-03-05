open Effect.Deep

(* ---- Effects ---- *)

type _ Effect.t += Yield : unit Effect.t
type _ Effect.t += Emit : int -> unit Effect.t
type _ Effect.t += Perform : int -> int Effect.t

(* ---- Timing ---- *)

let clock () = Unix.gettimeofday () *. 1e9 |> Int64.of_float

let bench name iters f =
  let elapsed = f iters in
  let ns_op = Int64.div elapsed (Int64.of_int iters) in
  let ms = Int64.to_float elapsed /. 1e6 in
  Printf.printf "%-40s %12d %14.3f %10Ld\n" name iters ms ns_op

(* ---- 1. Fiber create/destroy ---- *)

let bench_fiber_create_destroy iters =
  let start = clock () in
  for _ = 1 to iters do
    match_with (fun () -> Stdlib.Effect.perform Yield) ()
      { retc = (fun () -> ());
        exnc = (fun _ -> ());
        effc = fun (type a) (eff : a Effect.t) ->
          match eff with
          | Yield -> Some (fun (k : (a, _) continuation) ->
              discontinue k (Failure "done"))
          | _ -> None };
    ()
  done;
  Int64.sub (clock ()) start

(* ---- 2. Yield/resume round-trips ---- *)

let bench_yield_resume iters =
  let start = clock () in
  let _r = match_with (fun () ->
    for _ = 1 to iters do
      Stdlib.Effect.perform Yield
    done
  ) ()
    { retc = (fun () -> ());
      exnc = raise;
      effc = fun (type a) (eff : a Effect.t) ->
        match eff with
        | Yield -> Some (fun (k : (a, _) continuation) ->
            continue k ())
        | _ -> None }
  in
  Int64.sub (clock ()) start

(* ---- 3. Emit dispatch ---- *)

let bench_emit_dispatch iters =
  let counter = ref 0 in
  let start = clock () in
  match_with (fun () ->
    for i = 1 to iters do
      Stdlib.Effect.perform (Emit i)
    done
  ) ()
    { retc = (fun () -> ());
      exnc = raise;
      effc = fun (type a) (eff : a Effect.t) ->
        match eff with
        | Emit _ -> Some (fun (k : (a, _) continuation) ->
            incr counter;
            continue k ())
        | _ -> None };
  ignore (Sys.opaque_identity !counter);
  Int64.sub (clock ()) start

(* ---- 4. Perform dispatch ---- *)

let bench_perform_dispatch iters =
  let start = clock () in
  match_with (fun () ->
    for i = 1 to iters do
      let r = Stdlib.Effect.perform (Perform i) in
      ignore (Sys.opaque_identity r)
    done
  ) ()
    { retc = (fun () -> ());
      exnc = raise;
      effc = fun (type a) (eff : a Effect.t) ->
        match eff with
        | Perform _ -> Some (fun (k : (a, _) continuation) ->
            continue k 42)
        | _ -> None };
  Int64.sub (clock ()) start

(* ---- 5. Scheduler: N fibers x M performs ---- *)

let bench_scheduler iters =
  let n = 10 and m = 1000 in
  let start = clock () in
  for _ = 1 to iters do
    let q : (unit -> unit) Queue.t = Queue.create () in
    (* Spawn n fibers *)
    for _ = 1 to n do
      Queue.push (fun () ->
        match_with (fun () ->
          for j = 1 to m do
            let r = Stdlib.Effect.perform (Perform j) in
            ignore (Sys.opaque_identity r)
          done
        ) ()
          { retc = (fun () -> ());
            exnc = raise;
            effc = fun (type a) (eff : a Effect.t) ->
              match eff with
              | Perform _ -> Some (fun (k : (a, _) continuation) ->
                  Queue.push (fun () -> continue k 42) q)
              | _ -> None }
      ) q
    done;
    (* Run scheduler loop *)
    while not (Queue.is_empty q) do
      (Queue.pop q) ()
    done
  done;
  Int64.sub (clock ()) start

(* ---- 6. Multicore scheduler: N fibers x M performs, W domains ---- *)

(* Simple lock-protected deque (OCaml GC makes lock-free Chase-Lev impractical). *)
module Wsdeque : sig
  type 'a t
  val create : unit -> 'a t
  val push : 'a t -> 'a -> unit
  val pop : 'a t -> 'a option       (* LIFO — owner end *)
  val steal : 'a t -> 'a option     (* FIFO — thief end *)
end = struct
  type 'a t = {
    mu: Mutex.t;
    buf: 'a Queue.t;
  }
  let create () = { mu = Mutex.create (); buf = Queue.create () }
  let push t v = Mutex.lock t.mu; Queue.push v t.buf; Mutex.unlock t.mu
  (* pop from back = LIFO.  Queue only has FIFO pop, so we approximate
     with FIFO here; good enough for a benchmark comparison. *)
  let pop t =
    Mutex.lock t.mu;
    let r = if Queue.is_empty t.buf then None else Some (Queue.pop t.buf) in
    Mutex.unlock t.mu; r
  let steal t = pop t   (* same lock-based path *)
end

let bench_scheduler_multicore iters =
  let num_fibers = 40 and m = 1000 and num_workers = 4 in
  let start = clock () in
  for _ = 1 to iters do
    let deques = Array.init num_workers (fun _ -> Wsdeque.create ()) in
    let live = Atomic.make num_fibers in
    let shutdown = Atomic.make false in

    (* Distribute fibers round-robin *)
    for i = 0 to num_fibers - 1 do
      let wid = i mod num_workers in
      Wsdeque.push deques.(wid) (fun () ->
        match_with (fun () ->
          for j = 1 to m do
            let r = Stdlib.Effect.perform (Perform j) in
            ignore (Sys.opaque_identity r)
          done
        ) ()
          { retc = (fun () ->
              let prev = Atomic.fetch_and_add live (-1) in
              if prev <= 1 then Atomic.set shutdown true);
            exnc = raise;
            effc = fun (type a) (eff : a Effect.t) ->
              match eff with
              | Perform _ -> Some (fun (k : (a, _) continuation) ->
                  Wsdeque.push deques.(wid) (fun () -> continue k 42))
              | _ -> None })
    done;

    let worker_loop my_id =
      let rng = ref (my_id + 1) in
      let xorshift () =
        let x = !rng in
        let x = x lxor (x lsl 13) in
        let x = x lxor (x lsr 17) in
        let x = x lxor (x lsl 5) in
        rng := x land 0x7fffffff;  (* keep positive *)
        x
      in
      let try_steal () =
        let start = (xorshift ()) mod num_workers in
        let found = ref None in
        for i = 0 to num_workers - 1 do
          if Option.is_none !found then begin
            let tid = (start + i) mod num_workers in
            if tid <> my_id then
              match Wsdeque.steal deques.(tid) with
              | Some _ as v -> found := v
              | None -> ()
          end
        done;
        !found
      in
      let continue_running = ref true in
      while !continue_running do
        match Wsdeque.pop deques.(my_id) with
        | Some f -> f ()
        | None ->
          match try_steal () with
          | Some f -> f ()
          | None ->
            if Atomic.get shutdown then
              continue_running := false
            else
              Domain.cpu_relax ()
      done
    in

    (* Spawn worker domains 1..N-1 *)
    let domains = Array.init (num_workers - 1) (fun i ->
      Domain.spawn (fun () -> worker_loop (i + 1))
    ) in
    (* Worker 0 on current domain *)
    worker_loop 0;
    Array.iter Domain.join domains
  done;
  Int64.sub (clock ()) start

(* ---- Main ---- *)

let () =
  Printf.printf "\nOCaml 5 Effects\n";
  Printf.printf "%-40s %12s %14s %10s\n"
    "Benchmark" "Iterations" "Total (ms)" "ns/op";
  Printf.printf "%s\n" (String.make 78 '-');
  bench "fiber create/destroy"            100_000   bench_fiber_create_destroy;
  bench "yield/resume round-trips"      1_000_000   bench_yield_resume;
  bench "emit dispatch"                 1_000_000   bench_emit_dispatch;
  bench "perform dispatch"              1_000_000   bench_perform_dispatch;
  bench "scheduler 1W 10x1000 performs"       100   bench_scheduler;
  bench "scheduler 4W 40x1000 performs"        10   bench_scheduler_multicore;
  Printf.printf "\n"
