open Util
open Worker_manager
open Thread_pool

let map_jobs = Hashtbl.create 1000
let map_jobs_lock = Mutex.create ()
let map_results = Hashtbl.create 1000 
let map_results_lock = Mutex.create ()
let reduce_jobs = Hashtbl.create 1000
let reduce_jobs_lock = Mutex.create ()
let reduce_results = Hashtbl.create 1000
let reduce_results_lock = Mutex.create ()
let combine_ht = Hashtbl.create 1000

let map kv_pairs map_filename : (string * string) list = 
  let mapper_wm = initialize_mappers map_filename in
  let tpool = Thread_pool.create 60 in
  let (@) xs ys = List.rev_append (List.rev xs) ys in
  let ht_to_list ht =
    Hashtbl.fold (fun n (k,v) a -> a@[(n,(k,v))]) ht [] in
  (* to account for duplicate kv pairs and their results*)
  let rec number_kvs kv_list c = match kv_list with
    |[] -> []
    | (key,value) :: t -> [(c,(key,value))]@(number_kvs t (c+1)) in
  let num_kvs = number_kvs kv_pairs 0 in
  let rec assign_jobs num_kv_list = match num_kv_list with
    | [] -> (*print_endline "assign_jobs: done"*) () 
    | (num,(key,value)) :: t -> 
      let work () = 
      (* add job to hashtable of jobs being processed *)
      Mutex.lock map_jobs_lock;
      if (not(Hashtbl.mem map_jobs num)) 
      then Hashtbl.add map_jobs num (key,value);
      Mutex.unlock map_jobs_lock;
      let worker = Worker_manager.pop_worker mapper_wm in
      let result = Worker_manager.map worker key value in
      match result with
        | Some(list) -> 
           Worker_manager.push_worker mapper_wm worker;
          (* add successful result to results hashtable *)
          Mutex.lock map_results_lock;
          Mutex.lock map_jobs_lock;
          if (not(Hashtbl.mem map_results num)) 
          then begin Hashtbl.remove map_jobs num;
            Hashtbl.add map_results num list; 
            Mutex.unlock map_jobs_lock;
            Mutex.unlock map_results_lock end
          else Hashtbl.remove map_jobs num;
            Mutex.unlock map_results_lock; 
            Mutex.unlock map_jobs_lock
        |  None -> ()  in
      Thread_pool.add_work work tpool;
      assign_jobs t in
    let rec process_jobs kvs =  
      assign_jobs kvs;
      Thread.delay(0.1);
      Mutex.lock map_jobs_lock;
      let jobs_length = Hashtbl.length map_jobs in
      if (jobs_length > 0) then (
        let new_input = ht_to_list map_jobs in
        Mutex.unlock map_jobs_lock; 
        process_jobs new_input) 
      else( 
        Mutex.unlock map_jobs_lock;
        Thread_pool.destroy tpool;
        Worker_manager.clean_up_workers mapper_wm) in
    process_jobs num_kvs;
    let return_map_result ht = Hashtbl.fold (fun k v a -> a@[v]) ht [] in
    List.flatten (return_map_result map_results) 

let combine kv_pairs : (string * string list) list = 
  let rec add_to_ht kv_list = match kv_list with
    | [] -> ()
    | (k,v) :: t ->
    if (Hashtbl.mem combine_ht k) then( 
    Hashtbl.replace combine_ht k (v::(Hashtbl.find combine_ht k)); add_to_ht t)
    else (Hashtbl.add combine_ht k [v]; add_to_ht t) in 
  add_to_ht kv_pairs;
  let return_combine_list ht = 
    Hashtbl.fold (fun k v a -> (k,v) :: a) ht [] in
  return_combine_list combine_ht 

let reduce kvs_pairs reduce_filename : (string * string list) list =
  let reducer_wm = initialize_reducers reduce_filename in
  let tpool = Thread_pool.create 60 in
  let (@) xs ys = List.rev_append (List.rev xs) ys in
  let ht_to_list ht =
    Hashtbl.fold (fun n (k,v_list) a -> a@[(n,(k,v_list))]) ht [] in
  (* to account for duplicate kv pairs and their results*)
  let rec number_kvs kv_list c = match kv_list with
    |[] -> []
    | (key,value_list) :: t -> [(c,(key,value_list))]@(number_kvs t (c+1)) in
  let num_kvs = number_kvs kvs_pairs 0 in
  let rec assign_jobs num_kv_list = match num_kv_list with
    | [] -> ()
    | (num,(key,value_list)) :: t -> 
      let work () = 
      let worker = Worker_manager.pop_worker reducer_wm in
      (* add job to hashtable of jobs being processed *)
      Mutex.lock reduce_jobs_lock;
      if (not(Hashtbl.mem reduce_jobs num)) 
      then Hashtbl.add reduce_jobs num (key,value_list);
      Mutex.unlock reduce_jobs_lock;
      let result = Worker_manager.reduce worker key value_list in
      match result with
        | Some(list) -> 
           Worker_manager.push_worker reducer_wm worker;
          (* add successful result to results hashtable *)
          Mutex.lock reduce_jobs_lock;
          Mutex.lock reduce_results_lock;
          if (not(Hashtbl.mem reduce_results num)) then begin
            Hashtbl.add reduce_results num (key,list);
            Hashtbl.remove reduce_jobs num;
            Mutex.unlock reduce_results_lock; 
            Mutex.unlock reduce_jobs_lock end
          else Hashtbl.remove reduce_jobs num;
            Mutex.unlock reduce_results_lock; 
            Mutex.unlock reduce_jobs_lock
        |  None -> ()  in
      Thread_pool.add_work work tpool;
      assign_jobs t in
    let rec process_jobs kvs =  
      assign_jobs kvs;
      Thread.delay(0.1);
      Mutex.lock reduce_jobs_lock;
      let jobs_length = Hashtbl.length reduce_jobs in
      if (jobs_length > 0) then 
        let new_input = ht_to_list reduce_jobs in
        Mutex.unlock reduce_jobs_lock;
        process_jobs new_input
      else(
        Mutex.unlock reduce_jobs_lock;
        Thread_pool.destroy tpool; 
        Worker_manager.clean_up_workers reducer_wm) in
    process_jobs num_kvs;
    let return_reduce_result ht = Hashtbl.fold (fun n (k,l) a -> a@[(k,l)]) ht [] in
    return_reduce_result reduce_results 
  
let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

