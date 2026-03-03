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

(* ---- Main ---- *)

let () =
  Printf.printf "\nOCaml 5 Effects\n";
  Printf.printf "%-40s %12s %14s %10s\n"
    "Benchmark" "Iterations" "Total (ms)" "ns/op";
  Printf.printf "%s\n" (String.make 78 '-');
  bench "fiber create/destroy"        100_000   bench_fiber_create_destroy;
  bench "yield/resume round-trips"  1_000_000   bench_yield_resume;
  bench "emit dispatch"             1_000_000   bench_emit_dispatch;
  bench "perform dispatch"          1_000_000   bench_perform_dispatch;
  bench "scheduler 10x1000 performs"      100   bench_scheduler;
  Printf.printf "\n"
